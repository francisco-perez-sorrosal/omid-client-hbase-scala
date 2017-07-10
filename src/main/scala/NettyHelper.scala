package com.fps.omid.client.hbase

import java.util.concurrent.Executors

import akka.actor.ActorRef
import com.fps.omid.client.hbase.TSOClientProtocol._
import com.google.common.util.concurrent.ThreadFactoryBuilder
import com.typesafe.scalalogging.Logger
import org.apache.omid.proto.TSOProto
import org.apache.omid.tso.client.ConnectionException
import org.jboss.netty.bootstrap.ClientBootstrap
import org.jboss.netty.channel._
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory
import org.jboss.netty.handler.codec.frame.{LengthFieldBasedFrameDecoder, LengthFieldPrepender}
import org.jboss.netty.handler.codec.protobuf.{ProtobufDecoder, ProtobufEncoder}

object NettyHelper {

  val logger = Logger("NettyHelper")

  def initNettyClient(handlerActor: ActorRef): ClientBootstrap = {

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
    bootstrap.getPipeline.addLast("handler", new Handler(handlerActor))
    bootstrap

  }

  private class Handler(val fsm: ActorRef) extends SimpleChannelHandler {

    override def channelConnected(ctx: ChannelHandlerContext, e: ChannelStateEvent) {
      logger.debug("HANDLER (CHANNEL CONNECTED): Connection {}. Sending connected event to FSM", e)
      fsm ! Connected(e.getChannel)
    }

    @throws[Exception]
    override def channelDisconnected(ctx: ChannelHandlerContext, e: ChannelStateEvent) {
      logger.debug("HANDLER (CHANNEL DISCONNECTED): Connection {}. Sending error event to FSM", e)
      fsm ! ChannelDisconnected(new ConnectionException)
    }

    @throws[Exception]
    override def channelClosed(ctx: ChannelHandlerContext, e: ChannelStateEvent) {
      logger.debug("HANDLER (CHANNEL CLOSED): Connection {}. Sending channel closed event to FSM", e)
      fsm ! ChannelClosed(new ConnectionException)
    }

    override def messageReceived(ctx: ChannelHandlerContext, e: MessageEvent) {
      logger.debug("Message received {}", e.getMessage)
      if (e.getMessage.isInstanceOf[TSOProto.Response])
        fsm ! Response(e.getMessage.asInstanceOf[TSOProto.Response])
      else
        logger.warn("Received unknown message", e.getMessage)
    }

    @throws[Exception]
    override def exceptionCaught(ctx: ChannelHandlerContext, e: ExceptionEvent) {
      logger.error("Error on channel {}", ctx.getChannel, e.getCause)
      fsm ! ChannelError(e.getCause)
    }

  }

}
