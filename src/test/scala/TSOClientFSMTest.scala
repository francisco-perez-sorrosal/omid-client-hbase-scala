package com.fps.omid.client.hbase

import java.io.IOException
import java.net.{InetSocketAddress, ServerSocket, Socket}

import akka.actor.ActorSystem
import akka.actor.FSM.{CurrentState, SubscribeTransitionCallBack, Transition}
import akka.actor.SupervisorStrategy.Stop
import akka.testkit.{ImplicitSender, TestFSMRef, TestKit, TestProbe}
import com.fps.omid.client.hbase.TSOClientFSM._
import com.fps.omid.client.hbase.TSOClientProtocol.Request
import com.google.inject.Guice
import com.typesafe.scalalogging.Logger
import org.apache.omid.proto.TSOProto
import org.apache.omid.tso.client.AbortException
import org.apache.omid.tso.{TSOServer, TSOServerConfig, VoidLeaseManagementModule}
import org.jboss.netty.channel.Channel
import org.scalatest._
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.time.{Millis, Seconds, Span}

import scala.concurrent.duration.Duration
import scala.concurrent.{Future, Promise}
import scala.util.Try


class TSOClientFSMSpec extends TestKit(ActorSystem("tso-client-system"))
  with FunSpecLike with BeforeAndAfter with Matchers with Inside with ScalaFutures with ImplicitSender {

  val logger = Logger(classOf[TSOClientFSMSpec])

  private val tsoHost = "localhost"
  private val tsoPortForTest = getFreeLocalPort.get

  var tsoServer: TSOServer = null

  implicit override val patienceConfig = PatienceConfig(timeout = Span(5, Seconds), interval = Span(500, Millis))

  before {

    val tsoConfig = new TSOServerConfig
    tsoConfig.setMaxItems(1000)
    tsoConfig.setPort(tsoPortForTest)
    tsoConfig.setLeaseModule(new VoidLeaseManagementModule)

    val injector = Guice.createInjector(new TSOMockModule(tsoConfig))
    logger.info("Starting TSO Server")
    tsoServer = injector.getInstance(classOf[TSOServer])
    tsoServer.startAndWait
    waitForSocketListening(tsoHost, tsoPortForTest, 100)
    logger.info("Finished loading TSO")

  }

  after {

    logger.info("Shutting down TSO Server")
    tsoServer.stopAndWait()

  }

  describe("The TSO Client FSM") {

    it("should start in the Disconnected state") {

      val tsoAddr = new InetSocketAddress("localhost", tsoPortForTest)
      val tsoClientFSM = TestFSMRef(new TSOClientFSM(tsoAddr))

      tsoClientFSM.stateName should be(DisconnectedState)
      inside(tsoClientFSM.stateData) {
        case ConnectionData(_, tsoAddr, currentChannel, onTheFlyTimestampRequests, onTheFlyCommitRequests) =>
          currentChannel shouldBe empty
          onTheFlyTimestampRequests shouldBe empty
          onTheFlyCommitRequests shouldBe empty
      }
    }

    it("should finish in the Disconnected state") {

      val tsoAddr = new InetSocketAddress("localhost", tsoPortForTest)
      val tsoClientFSM = TestFSMRef(new TSOClientFSM(tsoAddr))

      tsoClientFSM.stateName should be(DisconnectedState)

      tsoClientFSM ! Stop

      inside(tsoClientFSM.stateData) {
        case ConnectionData(_, tsoAddr, currentChannel, onTheFlyTimestampRequests, onTheFlyCommitRequests) =>
          currentChannel shouldBe empty
          onTheFlyTimestampRequests shouldBe empty
          onTheFlyCommitRequests shouldBe empty
      }
    }

    it("should connect to server and return a start timestamp when sending Timestamp message") {

      val tsoAddr = new InetSocketAddress("localhost", tsoPortForTest)
      val tsoClientFSM = TestFSMRef(new TSOClientFSM(tsoAddr))

      val stateProbe = TestProbe()
      tsoClientFSM ! new SubscribeTransitionCallBack(this.testActor)

      var connectedChannel: Channel = null
      within(Duration(10, "seconds")) {
        val tsF = sendNewStartTimestamp(tsoClientFSM)

        expectMsg(CurrentState(tsoClientFSM, DisconnectedState))
        expectMsg(Transition(tsoClientFSM, DisconnectedState, ConnectingState))
        expectMsg(Transition(tsoClientFSM, ConnectingState, HandshakingState))
        connectedChannel = tsoClientFSM.stateData.currentChannel.get
        expectMsg(Transition(tsoClientFSM, HandshakingState, ConnectedState))

        whenReady(tsF) { result =>
          result shouldBe 1
        }
      }

      inside(tsoClientFSM.stateData) {
        case ConnectionData(_, _, currentChannel, onTheFlyTimestampRequests, onTheFlyCommitRequests) =>
          currentChannel shouldBe Some(connectedChannel)
          onTheFlyTimestampRequests shouldBe empty
          onTheFlyCommitRequests shouldBe empty
      }

    }

    it("should return a commit timestamp when sending a Commit message after a Timestamp is obtained") {

      val tsoAddr = new InetSocketAddress("localhost", tsoPortForTest)
      val tsoClientFSM = TestFSMRef(new TSOClientFSM(tsoAddr))

      within(Duration(10, "seconds")) {
        val stF = sendNewStartTimestamp(tsoClientFSM)

        whenReady(stF) { result =>
          result shouldBe 1
        }

        val startTimestamp = stF.value.get.get
        val ctF = sendNewCommitTimestamp(tsoClientFSM, startTimestamp)

        whenReady(ctF) { result =>
          result shouldBe 2
        }

      }

      inside(tsoClientFSM.stateData) {
        case ConnectionData(_, _, _, onTheFlyTimestampRequests, onTheFlyCommitRequests) =>
          onTheFlyTimestampRequests shouldBe empty
          onTheFlyCommitRequests shouldBe empty
      }

    }

    it("should throw an abort exception exception when sending a Commit message for a conflicting concurrent writeset") {

      val tsoAddr = new InetSocketAddress("localhost", tsoPortForTest)
      val tsoClientFSM = TestFSMRef(new TSOClientFSM(tsoAddr))

      within(Duration(10, "seconds")) {

        val st1F = sendNewStartTimestamp(tsoClientFSM)

        whenReady(st1F) { result =>
          result shouldBe 1
        }

        val st2F = sendNewStartTimestamp(tsoClientFSM)

        whenReady(st2F) { result =>
          result shouldBe 2
        }

        val conflictingWS = Set(DummyCellIdImpl(0xdeadbeefL))

        val ct1F = sendNewCommitTimestamp(tsoClientFSM, st1F.value.get.get, conflictingWS)

        whenReady(ct1F) { result =>
          result shouldBe 3
        }

        val ct2F = sendNewCommitTimestamp(tsoClientFSM, st2F.value.get.get, conflictingWS)

        whenReady(ct2F.failed) { ex =>

          ex shouldBe an[AbortException]

        }

      }

      inside(tsoClientFSM.stateData) {
        case ConnectionData(_, _, _, onTheFlyTimestampRequests, onTheFlyCommitRequests) =>
          onTheFlyTimestampRequests shouldBe empty
          onTheFlyCommitRequests shouldBe empty
      }

    }


  }

  @throws[IOException]
  @throws[InterruptedException]
  def waitForSocketListening(host: String, port: Int, sleepTimeMillis: Int) {

    var sock: Socket = null
    while (true) {
      try {
        sock = new Socket(host, port);
        if (sock != null) {
          return
        }
      } catch {
        case e: IOException => Thread.sleep(sleepTimeMillis)
      }
    }

  }

  @throws[IOException]
  def getFreeLocalPort: Try[Int] = Try {
    val socket = new ServerSocket(0)
    import com.fps.omid.utils.resources._
    for (s <- socket.autoDispose) {
      socket.setReuseAddress(true)
    }
    socket.getLocalPort
  }

  def sendNewStartTimestamp(fsm: TestFSMRef[TSOClientFSM.State, TSOClientFSM.ConnectionData, TSOClientFSM],
                            retries: Int = 1,
                            timeout: Duration = Duration(1, "second")): Future[Long] = {

    val requestBuilder = TSOProto.Request.newBuilder
    requestBuilder.setTimestampRequest(TSOProto.TimestampRequest.newBuilder.build)
    val p = Promise[Long]()
    fsm ! Request(RequestData(requestBuilder.build, retries, timeout, p))
    p.future

  }

  def sendNewCommitTimestamp(fsm: TestFSMRef[TSOClientFSM.State, TSOClientFSM.ConnectionData, TSOClientFSM],
                             startTimestamp: Long,
                             cells: Set[_ <: CellId] = Set.empty,
                             retries: Int = 1,
                             timeout: Duration = Duration(1, "second")): Future[Long] = {

    val requestBuilder = TSOProto.Request.newBuilder
    val commitbuilder = TSOProto.CommitRequest.newBuilder
    commitbuilder.setStartTimestamp(startTimestamp)
    cells.foreach(cell => commitbuilder.addCellId(cell.getCellId))
    requestBuilder.setCommitRequest(commitbuilder)
    val p = Promise[Long]()
    fsm ! Request(RequestData(requestBuilder.build, retries, timeout, p))
    p.future

  }

  case class DummyCellIdImpl(val cellId: Long) extends CellId {
    def getCellId: Long = cellId
  }

}