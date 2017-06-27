package com.fps.omid.client.hbase

import java.io.IOException
import java.net.InetSocketAddress
import java.util.concurrent.Executors

import akka.actor.{Actor, ActorLogging, ActorRef, LoggingFSM, PoisonPill, Props, ReceiveTimeout}
import com.fps.omid.client.hbase.RequestHolderActor.{ForceClose, ReceiveAbort, ReceiveRetry, ReceiveTimestamp}
import com.fps.omid.client.hbase.TSOClientFSM._
import com.fps.omid.client.hbase.TSOClientProtocol._
import com.google.common.util.concurrent.ThreadFactoryBuilder
import org.apache.omid.proto.TSOProto
import org.apache.omid.tso.client.{AbortException, ConnectionException, ServiceUnavailableException}
import org.jboss.netty.bootstrap.ClientBootstrap
import org.jboss.netty.channel._
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory
import org.jboss.netty.handler.codec.frame.{LengthFieldBasedFrameDecoder, LengthFieldPrepender}
import org.jboss.netty.handler.codec.protobuf.{ProtobufDecoder, ProtobufEncoder}

import scala.collection.immutable.Queue
import scala.collection.parallel.immutable
import scala.collection.parallel.immutable.ParHashMap
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
                            pendingRequests: Queue[RequestData],
                            onTheFlyTimestampRequests: Queue[ActorRef],
                            onTheFlyCommitRequests: immutable.ParHashMap[Long, ActorRef])

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

case class RequestData(protoContent: TSOProto.Request, var retries: Int, timeout: Duration, result: Promise[Long])

class TSOClientFSM(val tsoAddr: InetSocketAddress) extends LoggingFSM[State, ConnectionData] {

  startWith(DisconnectedState,
            ConnectionData(bootstrap = initNettyClient(),
                           tsoAddr = tsoAddr,
                           currentChannel = Option.empty,
                           pendingRequests = Queue.empty,
                           onTheFlyTimestampRequests = Queue.empty,
                           onTheFlyCommitRequests = ParHashMap.empty))

  when(DisconnectedState) {
    case Event(Request(content), stateData) =>
      val newRequests = stateData.pendingRequests.enqueue(content)
      log.info("Enqueuing request {}", content)
      tryToConnectToTSOServer(stateData.bootstrap, tsoAddr)
      goto(ConnectingState) using stateData.copy(pendingRequests = newRequests)
    case _ =>
      stay
  }

  when(ConnectingState) {
    case Event(Request(content), stateData) =>
      log.info("Enqueuing request {}", content)
      val newRequests = stateData.pendingRequests.enqueue(content)
      stay using stateData.copy(pendingRequests = newRequests)
    case Event(Connected(channel), stateData) =>
      log.info("channel {}", channel)
      goto(HandshakingState) using stateData.copy(currentChannel = Option(channel))
    case Event(ChannelClosed(exception), _) =>
      log.error("Channel Closed.", exception)
      goto(ConnectionFailedState)
    case Event(ChannelDisconnected(exception), _) =>
      log.error("Channel Disconnected.", exception)
      goto(DisconnectedState) using stateData
  }

  when(HandshakingState, stateTimeout = Duration(30, "seconds")) {
    case Event(StateTimeout, _) =>
      log.warning("Timeout while waiting for Handshake response...")
      goto(ClosingState)
    case Event(Request(content), stateData) =>
      log.info("Enqueuing request while waiting for Handshake... {}", content)
      val newRequests = stateData.pendingRequests.enqueue(content)
      stay using stateData.copy(pendingRequests = newRequests)
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
      triggerPendingRequests(self, stateData)
      stay using stateData.copy(pendingRequests = Queue.empty)
    case Event(Request(content), stateData) =>
      triggerPendingRequests(self, stateData)
      if (content.protoContent.hasTimestampRequest) {
        val newTimestampRequestsQueue = sendTimestampRequest(self, content, stateData)
        stay using stateData.copy(pendingRequests = Queue.empty, onTheFlyTimestampRequests = newTimestampRequestsQueue)
      } else if (content.protoContent.hasCommitRequest) {
        val newCommitRequestsMap = sendCommitRequest(self, content, stateData)
        stay using stateData.copy(pendingRequests = Queue.empty, onTheFlyCommitRequests = newCommitRequestsMap)
      } else {
        content.result.failure(new IllegalArgumentException("Unknown request type"))
        stay
      }
    case Event(Response(content), stateData) =>
      if (content.hasTimestampResponse) {
        val newQueueOp = handleTimestampResponse(self, content, stateData)
        if (!newQueueOp.isEmpty) {
          stay using stateData.copy(onTheFlyTimestampRequests = newQueueOp.get)
        } else {
          stay
        }
      } else if (content.hasCommitResponse) {
        val newMapOp = handleCommitResponse(self, content, stateData)
        if (!newMapOp.isEmpty) {
          stay using stateData.copy(onTheFlyCommitRequests = newMapOp.get)
        } else {
          stay
        }
      } else {
        stay
      }
    case Event(ChannelClosed(ex), _) =>
      log.error("Channel closed received.", ex)
      goto(ClosingState)
    case Event(ChannelDisconnected(ex), stateData) =>
      log.error("Channel disconnected received.", ex)
      goto(ClosingState) using stateData.copy(currentChannel = Option.empty,
                                              onTheFlyTimestampRequests = Queue.empty,
                                              onTheFlyCommitRequests = ParHashMap.empty)
  }

  when(ClosingState) {
    case Event(Request(content), stateData) =>
      val newRequests = stateData.pendingRequests.enqueue(content)
      log.info("Sorry, closing current channel. Enqueuing this request {}. Pending requests = {}", content, newRequests.size)
      stay using stateData.copy(pendingRequests = newRequests)
    case Event(ChannelClosed, _) =>
      log.info("Channel closed received!!!")
      goto(DisconnectedState) using stateData.copy(pendingRequests = filterUnretriable(stateData.pendingRequests))
    case _ =>
      goto(DisconnectedState) using stateData.copy(pendingRequests = filterUnretriable(stateData.pendingRequests))
  }

  when(ConnectionFailedState) {
    case Event(Request(content), _) =>
      content.result.failure(new ConnectionException())
      stay
    case Event(ChannelDisconnected, _) =>
      stay
    case _ =>
      goto(DisconnectedState) using stateData.copy(pendingRequests = filterUnretriable(stateData.pendingRequests))
  }

  onTransition {
    case _ -> HandshakingState =>
      sendHandshakingRequest(nextStateData.currentChannel.get)
    case _ -> ClosingState =>
      if (stateData.currentChannel.isEmpty) {
        log.warning("Attempting to close a channel that does not exist!!!")
      } else {
        log.info("Closing channel {}", stateData.currentChannel.get)
        stateData.currentChannel.get.close()
      }
    case _ -> DisconnectedState =>
      retry(stateData)

  }

  onTermination {
    case StopEvent(_, _, stateData) => log.info("TSOClient FSM terminated")
  }

  initialize()

  // ----------------------------------------------------------------------------------------------------------------
  // Helper classes & methods
  // ----------------------------------------------------------------------------------------------------------------

  def initNettyClient(): ClientBootstrap = {

    val factory : ChannelFactory = new NioClientSocketChannelFactory(
      Executors.newCachedThreadPool(new ThreadFactoryBuilder().setNameFormat("tsoclient-boss-%d").build),
      Executors.newCachedThreadPool(new ThreadFactoryBuilder().setNameFormat("tsoclient-worker-%d").build),
      1)

    val bootstrap : ClientBootstrap = new ClientBootstrap(factory)
    bootstrap.setOption("tcpNoDelay", true)
    bootstrap.setOption("keepAlive", true)
    bootstrap.setOption("reuseAddress", true)
    bootstrap.setOption("connectTimeoutMillis", 100)
    bootstrap.getPipeline.addLast("lengthbaseddecoder", new LengthFieldBasedFrameDecoder(8 * 1024, 0, 4, 0, 4))
    bootstrap.getPipeline.addLast("lengthprepender", new LengthFieldPrepender(4))
    bootstrap.getPipeline.addLast("protobufdecoder", new ProtobufDecoder(TSOProto.Response.getDefaultInstance))
    bootstrap.getPipeline.addLast("protobufencoder", new ProtobufEncoder)
    bootstrap.getPipeline.addLast("handler", new Handler(self))
    bootstrap

  }

  def sendHandshakingRequest(channel: Channel) = {
    val handshake = TSOProto.HandshakeRequest.newBuilder();
    // Add the required handshake capabilities when necessary
    handshake.setClientCapabilities(TSOProto.Capabilities.newBuilder().build())
    log.info("Sending handshaking request to TSO through channel {}", channel)
    channel.write(TSOProto.Request.newBuilder().setHandshakeRequest(handshake.build()).build());
  }

  private def triggerPendingRequests(fsm: ActorRef, stateData: ConnectionData) = {
    stateData.pendingRequests.map(req => {
      log.info("Sending pending request {}", req)
      fsm ! Request(req)
    })
  }

  def filterUnretriable(pendingRequests: Queue[RequestData]): Queue[RequestData] = {
    pendingRequests.filter(req => req.retries > 0)
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

  private def sendTimestampRequest(fsm: ActorRef, request: RequestData, state: ConnectionData): Queue[ActorRef] = {

    sendRequest(fsm, request, state)
    val tsActor = context.actorOf(RequestHolderActor.props(request))
    state.onTheFlyTimestampRequests.enqueue(tsActor)

  }

  private def sendCommitRequest(fsm: ActorRef, request: RequestData, state: ConnectionData): ParHashMap[Long, ActorRef] = {

    sendRequest(fsm, request, state)
    val commitReq = request.protoContent.getCommitRequest
    val commitActor = context.actorOf(RequestHolderActor.props(request))
    state.onTheFlyCommitRequests + (commitReq.getStartTimestamp -> commitActor)

  }

  private def sendRequest(fsm: ActorRef, request: RequestData, state: ConnectionData): Unit = {
    val f = state.currentChannel.get.write(request.protoContent)
    f.addListener(new ChannelFutureListener() {
      def operationComplete(future: ChannelFuture) {
        if (!future.isSuccess) fsm ! ChannelDisconnected(future.getCause)
      }
    })
  }

  private def handleTimestampResponse(fsm: ActorRef, response: TSOProto.Response, state: ConnectionData): Option[Queue[ActorRef]] = {

      if (state.onTheFlyTimestampRequests.size == 0) {
        log.error("Received timestamp response when no requests outstanding")
        Option.empty
      } else {
        val (actor, newQueue) = state.onTheFlyTimestampRequests.dequeue
        actor ! ReceiveTimestamp(response.getTimestampResponse.getStartTimestamp)
        Option(newQueue)
      }

  }

  private def handleCommitResponse(fsm: ActorRef, response: TSOProto.Response, state: ConnectionData): Option[ParHashMap[Long, ActorRef]] = {

    val startTimestamp = response.getCommitResponse.getStartTimestamp
    val actorOp = state.onTheFlyCommitRequests.get(startTimestamp)
    if (actorOp.isEmpty) {
      log.debug("Received commit response for request that doesn't exist. Start TS: {}", startTimestamp)
      Option.empty
    } else {
      if (response.getCommitResponse.getAborted) {
        actorOp.get ! ReceiveAbort
      } else {
        actorOp.get ! ReceiveTimestamp(response.getCommitResponse.getCommitTimestamp)
      }
      Option(state.onTheFlyCommitRequests - (startTimestamp))
    }
  }

  private def retry(state: ConnectionData) {
    log.info("Configuring retries...")
    state.pendingRequests.map(request => if (request.retries > 0) {
      log.info("Timeout for request {}", request)
      request.retries -= 1
    } else {
      request.result.failure(
        new IllegalStateException("Number of retries exceeded. This request failed permanently: " + request))
    })
    state.onTheFlyTimestampRequests.map(actor => actor ! ReceiveRetry)
    state.onTheFlyCommitRequests.valuesIterator.map(actor => actor ! ReceiveRetry)
  }

  private def abortOnTheFlyTx(state: ConnectionData) {
    log.warning("Aborting all on the fly transactions...")
    state.onTheFlyTimestampRequests.map(actor => actor ! ForceClose)
    state.onTheFlyCommitRequests.valuesIterator.map(actor => actor ! ForceClose)
  }

  private class Handler(val fsm: ActorRef) extends SimpleChannelHandler {

    override def channelConnected(ctx: ChannelHandlerContext, e: ChannelStateEvent) {
      log.debug("HANDLER (CHANNEL CONNECTED): Connection {}. Sending connected event to FSM", e)
      fsm ! Connected(e.getChannel)
    }

    @throws[Exception]
    override def channelDisconnected(ctx: ChannelHandlerContext, e: ChannelStateEvent) {
      log.debug("HANDLER (CHANNEL DISCONNECTED): Connection {}. Sending error event to FSM", e)
      fsm ! ChannelDisconnected(new ConnectionException)
    }

    @throws[Exception]
    override def channelClosed(ctx: ChannelHandlerContext, e: ChannelStateEvent) {
      log.debug("HANDLER (CHANNEL CLOSED): Connection {}. Sending channel closed event to FSM", e)
      fsm ! ChannelClosed(new ConnectionException)
    }

    override def messageReceived(ctx: ChannelHandlerContext, e: MessageEvent) {
      log.info("Message received {}", e.getMessage)
      if (e.getMessage.isInstanceOf[TSOProto.Response])
        fsm ! Response(e.getMessage.asInstanceOf[TSOProto.Response])
      else
        log.warning("Received unknown message", e.getMessage)
    }

    @throws[Exception]
    override def exceptionCaught(ctx: ChannelHandlerContext, e: ExceptionEvent) {
      log.error("Error on channel {}", ctx.getChannel, e.getCause)
      fsm ! ChannelError(e.getCause)
    }

  }

}

class RequestHolderActor(val request: RequestData) extends Actor with ActorLogging {

  context.setReceiveTimeout(request.timeout)

  def receive = {

    case ReceiveTimestamp(ts) =>
      if (request.protoContent.hasTimestampRequest || request.protoContent.hasCommitRequest) {
        log.info("setting promise with ts {}", ts)
        request.result.success(ts)
      } else {
        log.warning("Unknown response ts received ({}) for request {}", ts, request)
      }
      self ! PoisonPill

    case ReceiveAbort =>
      if (request.protoContent.hasCommitRequest) {
        request.result.failure(new AbortException)
      } else {
        log.warning("Abort message received for a non-commit request {}", request)
      }
      self ! PoisonPill

    case ReceiveRetry =>
      if (request.retries > 0) {
        request.retries -= 1
        log.info("Retrying request {}", request)
        sender ! Request(request)
      } else {
        log.info("=======================================================================================")
        request.result.failure(
          new ServiceUnavailableException("Retries exceeded. Request " + request + " failed permanently"))
        self ! PoisonPill
      }
    case ReceiveTimeout =>
      if (request.retries > 0) {
        log.info("Timeout for request {}", request)
        request.retries -= 1
        sender ! Request(request)
      } else {
        request.result.failure(
          new IllegalStateException("Number of retries exceeded. This request failed permanently: " + toString))
      }
      self ! PoisonPill

    case ForceClose =>
      request.result.failure(new IOException("Request " + request + " aborted by TSO client"))
      self ! PoisonPill
  }

  override def postStop() {
    log.info("RequestHolderActor for {} stopped", toString)
  }

  override def toString = {
    var info = "Request type "
    if (request.protoContent.hasTimestampRequest)
      info += "[Timestamp]"
    else if (request.protoContent.hasCommitRequest)
      info += "[Commit] Start TS -> " + request.protoContent.getCommitRequest.getStartTimestamp
    else
      info += "NONE"
    info
  }

}

object RequestHolderActor {

  def props(request: RequestData): Props = Props(new RequestHolderActor(request))

  case class ReceiveTimestamp(ts: Long)
  case class ReceiveAbort()
  case class ReceiveRetry()
  case class ForceClose()

}