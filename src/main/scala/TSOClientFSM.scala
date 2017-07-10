package com.fps.omid.client.hbase

import java.io.IOException
import java.net.InetSocketAddress
import java.util.concurrent.{CountDownLatch, TimeUnit}

import akka.actor.{Actor, ActorLogging, ActorRef, LoggingFSM, Props, ReceiveTimeout}
import akka.util.Timeout
import com.fps.omid.client.hbase.RequestHolderActor._
import com.fps.omid.client.hbase.RequestStatus.RequestStatus
import com.fps.omid.client.hbase.TSOClientFSM._
import com.fps.omid.client.hbase.TSOClientProtocol._
import org.apache.omid.proto.TSOProto
import org.apache.omid.tso.client.AbortException
import org.jboss.netty.bootstrap.ClientBootstrap
import org.jboss.netty.channel._

import scala.collection.mutable.{HashMap, Queue}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Promise
import scala.concurrent.duration.Duration
import scala.util.{Failure, Success}

object TSOClientFSM {

  // FSM States
  sealed trait State
  case object DisconnectedState extends State
  case object ConnectingState extends State
  case object HandshakingState extends State
  case object HandshakeFailedState extends State
  case object ConnectionFailedState extends State
  case object ConnectedState extends State
  case object ClosingState extends State

  case class ConnectionData(bootstrap: ClientBootstrap,
                            tsoAddr: InetSocketAddress,
                            currentChannel: Option[Channel],
                            onTheFlyTimestampRequests: Queue[ActorRef],
                            onTheFlyCommitRequests: HashMap[Long, ActorRef])

}

object TSOClientProtocol {

  trait UserInteraction
  trait NettyInteraction

  case class Request(content: RequestData) extends UserInteraction
  case class Close() extends UserInteraction

  case class Connected(channel: Channel) extends NettyInteraction
  case class ChannelClosed(t: Throwable) extends NettyInteraction
  case class ChannelDisconnected(t: Throwable) extends NettyInteraction
  case class ChannelError(t: Throwable) extends NettyInteraction
  case class Response(content: TSOProto.Response) extends NettyInteraction

}

object RequestStatus extends Enumeration {
  type RequestStatus = Value
  val NotSent, Sent, TimedOut = Value
}

object RequestType extends Enumeration {
  type RequestType = Value
  val Timestamp, Commit, Handshake = Value
}

case class RequestData(protoContent: TSOProto.Request,
                       var status: RequestStatus,
                       var retries: Int,
                       timeout: Duration,
                       result: Promise[Long]) {

  val typeOf = {
    if (protoContent.hasHandshakeRequest) {
      RequestType.Handshake
    } else if (protoContent.hasTimestampRequest) {
      RequestType.Timestamp
    } else {
      RequestType.Commit
    }
  }

}

class TSOClientFSM(val tsoAddr: InetSocketAddress) extends LoggingFSM[State, ConnectionData] {

  import akka.pattern.ask

  startWith(DisconnectedState,
            ConnectionData(bootstrap = NettyHelper.initNettyClient(self),
                           tsoAddr = tsoAddr,
                           currentChannel = Option.empty,
                           onTheFlyTimestampRequests = Queue.empty,
                           onTheFlyCommitRequests = HashMap.empty))

  when(DisconnectedState) {
    case Event(Request(content), stateData) =>
      tryToConnectToTSOServer(stateData.bootstrap, tsoAddr)
      createAndStoreRequestHolder(content, stateData)
      goto(ConnectingState)
    case _ =>
      stay
  }

  when(ConnectingState) {
    case Event(Request(content), stateData) =>
      createAndStoreRequestHolder(content, stateData)
      stay
    case Event(Connected(channel), stateData) =>
      log.info("New Netty Channel {} connected", channel)
      goto(HandshakingState) using stateData.copy(currentChannel = Option(channel))
    case Event(ChannelClosed(exception), _) =>
      log.error("Netty Channel Closed.", exception)
      goto(ConnectionFailedState)
    case Event(ChannelDisconnected(exception), _) =>
      log.error("Netty Channel Disconnected.", exception)
      goto(DisconnectedState)
  }

  when(HandshakingState, stateTimeout = Duration(30, "seconds")) {
    case Event(StateTimeout, _) =>
      log.warning("Timeout while waiting for Handshake response...")
      goto(ClosingState)
    case Event(Request(content), stateData) =>
      createAndStoreRequestHolder(content, stateData)
      stay
    case Event(Response(content), stateData) =>
      if (content.hasHandshakeResponse && content.getHandshakeResponse.getClientCompatible) {
       goto(ConnectedState)
      } else {
       log.error("Non-expected response while waiting for handshake completion: {}", content)
       goto(ClosingState)
      }
    case _ =>
      goto(ClosingState)
  }

  when(ConnectedState, stateTimeout = Duration(3, "second")) {

    case Event(StateTimeout, stateData) =>
      log.warning("Timeout while waiting for user requests. Looking for pending requests...")
      stay
    case Event(Request(content), stateData) =>
      createAndStoreRequestHolder(content, stateData) match {
        case Some(requestHolder) => requestHolder ! SendRequestThrough(stateData.currentChannel.get)
        case None =>
          log.error("Request Holder Actor for {} was not created", content)
          content.result.failure(new IllegalArgumentException("Unknown request type"))
      }
      stay

    case Event(Response(content), stateData) =>
      if (content.hasTimestampResponse) {
        handleTimestampResponse(content, stateData)
      } else if (content.hasCommitResponse) {
        handleCommitResponse(content, stateData)
      }
      stay
    case Event(ChannelClosed(ex), _) =>
      log.error("Netty Channel closed!", ex)
      goto(ClosingState) using stateData.copy(currentChannel = Option.empty)
    case Event(ChannelDisconnected(ex), stateData) =>
      log.error("Netty Channel disconnected!", ex)
      goto(ClosingState) using stateData.copy(currentChannel = Option.empty)
  }

  when(ClosingState) {
    case Event(Request(content), stateData) =>
      createAndStoreRequestHolder(content, stateData)
      stay
    case Event(ChannelClosed(ex), _) =>
      log.error("Channel closed!", ex)
      goto(DisconnectedState)
    case _ =>
      goto(DisconnectedState)
  }

  when(ConnectionFailedState) {
    case Event(Request(content), stateData) =>
      createAndStoreRequestHolder(content, stateData)
      stay
    case _ =>
      goto(DisconnectedState)
  }

  onTransition {
    case HandshakingState -> ConnectedState =>
      sendPendingRequests(stateData)
    case _ -> HandshakingState =>
      sendHandshakingRequest(nextStateData.currentChannel.get)
    case _ -> ClosingState =>
      if (!stateData.currentChannel.isEmpty && stateData.currentChannel.get.isConnected) {
        log.info("Closing channel {}", stateData.currentChannel.get)
        stateData.currentChannel.get.close()
      } else {
        log.info("Channel already closed")
      }

  }

  onTermination {
    case StopEvent(_, _, stateData) => log.info("TSOClient FSM terminated")
  }

  initialize()

  // ----------------------------------------------------------------------------------------------------------------
  // Helper classes & methods
  // ----------------------------------------------------------------------------------------------------------------

  def sendHandshakingRequest(channel: Channel) = {
    val handshake = TSOProto.HandshakeRequest.newBuilder()
    // Add the required handshake capabilities when necessary
    handshake.setClientCapabilities(TSOProto.Capabilities.newBuilder().build())
    log.info("Sending handshaking request to TSO through channel {}", channel)
    channel.write(TSOProto.Request.newBuilder().setHandshakeRequest(handshake.build()).build());
  }

  private def createAndStoreRequestHolder(content: RequestData, stateData: ConnectionData): Option[ActorRef] = {
    content.typeOf match {
      case RequestType.Timestamp =>
        log.info("Mapping timestamp request {}", content)
        val tsRequestHolder = createHolderActorFor(content)
        stateData.onTheFlyTimestampRequests.enqueue(tsRequestHolder)
        Option(tsRequestHolder)
      case RequestType.Commit =>
        log.info("Mapping commit request {}", content)
        val id = content.protoContent.getCommitRequest.getStartTimestamp
        val tsRequestHolder = createHolderActorFor(content)
        stateData.onTheFlyCommitRequests += (id -> tsRequestHolder)
        Option(tsRequestHolder)
      case _ =>
        log.error("Wrong request {}", content)
        Option.empty
    }
  }

  private def sendPendingRequests(stateData: ConnectionData) = {

    def sendTo(requestHolder: ActorRef): Unit = {
      log.info("{} sending pending request", requestHolder)
      requestHolder ! SendRequestThrough(stateData.currentChannel.get)
    }

    stateData.onTheFlyTimestampRequests.map(sendTo)
    stateData.onTheFlyCommitRequests.values.map(sendTo)

  }

  private def tryToConnectToTSOServer(bootstrap: ClientBootstrap, tsoAddress: InetSocketAddress) = {
    log.info("Trying to connect to TSO [{}]", tsoAddress)
    val channelFuture = bootstrap.connect(tsoAddress)
    channelFuture.addListener(new ChannelFutureListener() {
      @throws[Exception]
      def operationComplete(channelFuture: ChannelFuture) {
        if (channelFuture.isSuccess)
          log.info("Connection to TSO [{}] established. Channel {}", tsoAddress, channelFuture.getChannel)
        else
          log.error("Failed connection attempt to TSO [{}] failed. Channel {}", tsoAddress, channelFuture.getChannel)
      }
    })
  }

  private def createHolderActorFor(request: RequestData) = context.actorOf(RequestHolderActor.props(request))

  private def handleTimestampResponse(response: TSOProto.Response, state: ConnectionData): Unit = {

      if (state.onTheFlyTimestampRequests.isEmpty) {
        log.error("Received timestamp response when no requests outstanding")
      } else {
        var isRequestCompleted = false
        while(!isRequestCompleted) {
          val actor = state.onTheFlyTimestampRequests.dequeue()
          val latch: CountDownLatch = new CountDownLatch(1)
          implicit val timeout = Timeout(1, TimeUnit.SECONDS)
          val fut = actor ? GetRequest
          fut onComplete {
            case Success(request: RequestData) =>
              request.status match {
                case RequestStatus.NotSent =>
                  log.warning("Re-enqueuing not started request {}", request)
                  state.onTheFlyTimestampRequests.enqueue(actor)
                case RequestStatus.Sent =>
                  log.info("Setting success for request {}", request)
                  request.result.success(response.getTimestampResponse.getStartTimestamp)
                  isRequestCompleted = true
                case RequestStatus.TimedOut =>
                  log.info("Setting failure for request {}", request)
                  request.result.failure(new IOException("Number of request retries to TSO exceeded: " + request))
              }
              latch.countDown()
            case Failure(ex) =>
              log.error("Error retrieving request data from actor {}", actor, ex)
              latch.countDown()
          }
          latch.await()
        }

      }

  }

  private def handleCommitResponse(response: TSOProto.Response, state: ConnectionData): Unit = {

    val startTimestamp = response.getCommitResponse.getStartTimestamp
    val actorOp = state.onTheFlyCommitRequests.get(startTimestamp)
    if (actorOp.isEmpty) {
      log.debug("Received commit response for request that doesn't exist. Start TS: {}", startTimestamp)
    } else {
      val latch: CountDownLatch = new CountDownLatch(1)
      implicit val timeout = Timeout(1, TimeUnit.SECONDS)
      val fut = actorOp.get ? GetRequest
      fut onComplete {
        case Success(request: RequestData) =>
          request.status match {
            case RequestStatus.NotSent =>
              log.error("Uhmmmm this request should have been marked as sent to TSO {}", request)
            case RequestStatus.Sent =>
              if (response.getCommitResponse.getAborted) {
                request.result.failure(new AbortException)
              } else {
                request.result.success(response.getCommitResponse.getCommitTimestamp)
              }
            case RequestStatus.TimedOut =>
              request.result.failure(new IOException("Number of request retries to TSO exceeded: " + request))
          }
          state.onTheFlyCommitRequests -= (startTimestamp)
          latch.countDown()
        case Failure(ex) =>
          log.error("Error retrieving request data from actor {}. Start TS: {}", actorOp.get, startTimestamp, ex)
          latch.countDown()
      }
      latch.await()
    }
  }

}

class RequestHolderActor(val request: RequestData) extends Actor with ActorLogging {

  var currentChannel: Option[Channel] = None

  context.setReceiveTimeout(request.timeout)

  def receive = waitingConnection

  def waitingConnection: Receive = {

    case SendRequestThrough(channel) =>
      currentChannel = Option(channel)
      sendRequest(currentChannel.get)
      request.status = RequestStatus.Sent
      context.become(waitingResponse)

    case ReceiveTimeout =>
      if (request.retries > 0) {
        log.info("{} Timeout for request {}", self, request)
        request.retries -= 1
      } else {
        request.status = RequestStatus.TimedOut
        context.setReceiveTimeout(Duration.Undefined)
        context.become(timedOut)
      }

    case ForceClose =>
      context.stop(self)

  }

  def waitingResponse: Receive = {

    case SendRequestThrough(channel) =>
      currentChannel = Option(channel)
      request.status = RequestStatus.Sent
      sendRequest(currentChannel.get)

    case GetRequest =>
      context.setReceiveTimeout(Duration.Undefined)
      sender ! request
      context.stop(self)

    case ReceiveTimeout =>
      if (request.retries > 0) {
        log.info("{} Timeout for request {}", self, toString)
        request.retries -= 1
      } else {
        request.status = RequestStatus.TimedOut
        context.setReceiveTimeout(Duration.Undefined)
        context.become(timedOut)
      }

    case ForceClose =>
      context.stop(self)

  }

  def timedOut: Receive = {

    case SendRequestThrough(channel) =>
      log.info("{} Waiting my death... Doing nothing for request {}", self, toString)

    case GetRequest =>
      sender ! request
      context.stop(self)

    case ForceClose =>
      context.stop(self)

  }

  private def sendRequest(channel: Channel): Unit = {
    val f = channel.write(request.protoContent)
    f.addListener(new ChannelFutureListener() {
      def operationComplete(future: ChannelFuture) {
        if (!future.isSuccess) {
          log.error("{} Couldn't send request through channel {}", self, channel)
        }
      }
    })
  }

  override def postStop() {
    log.info("{} for {} stopped", self, toString)
  }

  override def toString = {
    var info =  "Request [" + request.typeOf + "] "
    request.typeOf match {
      case RequestType.Commit =>
        info += request.protoContent.getCommitRequest.getStartTimestamp
      case _ => // Do nothing
    }
    info
  }

}

object RequestHolderActor {

  def props(request: RequestData): Props = Props(new RequestHolderActor(request))

  case class SendRequestThrough(channel: Channel)
  case class GetRequest()
  case class ForceClose()

}