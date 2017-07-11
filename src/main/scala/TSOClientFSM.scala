package com.fps.omid.client.hbase

import java.io.IOException
import java.net.InetSocketAddress

import akka.actor.{Actor, ActorLogging, ActorRef, LoggingFSM, Props, ReceiveTimeout}
import com.fps.omid.client.hbase.RequestHolderActor._
import com.fps.omid.client.hbase.TSOClientFSM._
import com.fps.omid.client.hbase.TSOClientProtocol._
import org.apache.omid.proto.TSOProto
import org.apache.omid.tso.client.AbortException
import org.jboss.netty.bootstrap.ClientBootstrap
import org.jboss.netty.channel._

import scala.collection.mutable.{HashMap, Queue}
import scala.concurrent.Promise
import scala.concurrent.duration.Duration

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

object RequestType extends Enumeration {
  type RequestType = Value
  val Timestamp, Commit, Handshake = Value
}

case class RequestData(protoContent: TSOProto.Request,
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
    case Event(Response(content), _) =>
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
      handleResponse(content, stateData)
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

    stateData.onTheFlyTimestampRequests.foreach(sendTo)
    stateData.onTheFlyCommitRequests.values.foreach(sendTo)

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

  private def handleResponse(response: TSOProto.Response, state: ConnectionData): Unit = {

    def locateManagingActor(content: TSOProto.Response): Option[ActorRef] = {
      if (content.hasTimestampResponse) {
        if (state.onTheFlyTimestampRequests.isEmpty) {
          log.error("Received timestamp response when no requests outstanding")
          Option.empty
        } else {
          Option(state.onTheFlyTimestampRequests.dequeue())
        }
      } else if (content.hasCommitResponse) {
        val startTimestamp = response.getCommitResponse.getStartTimestamp
        val actorOp = state.onTheFlyCommitRequests.remove(startTimestamp)
        if (actorOp.isEmpty) {
          log.error("Received commit response for request that doesn't exist. Start TS: {}", startTimestamp)
          Option.empty
        } else {
          Option(actorOp.get)
        }
      } else {
        log.error("Received unknown response. Can't locate actor for it")
        Option.empty
      }
    }

    val managingActor = locateManagingActor(response)
    if (managingActor.isDefined) {
      managingActor.get ! ManageAndPrepare(response)
      managingActor.get ! ForceClose
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
      context.become(waitingResponse)

    case ReceiveTimeout =>
      if (request.retries > 0) {
        log.info("{} Timeout for request {}", self, request)
        request.retries -= 1
      } else {
        context.setReceiveTimeout(Duration.Undefined)
        context.become(timedOut)
      }

    case ForceClose =>
      context.stop(self)

  }

  def waitingResponse: Receive = {

    case SendRequestThrough(channel) =>
      currentChannel = Option(channel)
      sendRequest(currentChannel.get)

    case ManageAndPrepare(response) =>
      context.setReceiveTimeout(Duration.Undefined)
      request.typeOf match {
        case RequestType.Timestamp =>
          request.result.success(response.getTimestampResponse.getStartTimestamp)
        case RequestType.Commit =>
          if (response.getCommitResponse.getAborted) {
            request.result.failure(new AbortException)
          } else {
            request.result.success(response.getCommitResponse.getCommitTimestamp)
          }
        case _ =>
          log.error("Wrong request hold! {}", request)
      }

    case ReceiveTimeout =>
      if (request.retries > 0) {
        log.info("{} Timeout for request {}", self, toString)
        request.retries -= 1
      } else {
        context.setReceiveTimeout(Duration.Undefined)
        context.become(timedOut)
      }

    case ForceClose =>
      context.stop(self)

  }

  def timedOut: Receive = {

    case SendRequestThrough(channel) =>
      log.info("{} Waiting my death... Doing nothing for request {}", self, toString)

    case ManageAndPrepare(response) =>
      request.typeOf match {
        case RequestType.Timestamp =>
          log.info("Setting failure for request {} and recycling received Start TS in response", request)
          request.result.failure(new IOException("Number of request retries to TSO exceeded: " + request))
          sender ! Response(response)
        case RequestType.Commit =>
          request.result.failure(new IOException("Number of request retries to TSO exceeded: " + request))
        case _ =>
          log.error("Wrong request hold! {}", request)
      }

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
  case class ManageAndPrepare(response: TSOProto.Response)
  case class ForceClose()

}