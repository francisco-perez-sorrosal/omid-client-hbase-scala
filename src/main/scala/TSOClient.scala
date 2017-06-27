package com.fps.omid.client.hbase

import java.io.Closeable
import java.net.InetSocketAddress

import akka.actor.{ActorSystem, Props}
import com.fps.omid.client.hbase.TSOClientProtocol.Request
import com.google.common.net.HostAndPort
import com.typesafe.scalalogging.Logger
import org.apache.omid.proto.TSOProto
import org.apache.omid.tso.client.OmidClientConfiguration

import scala.concurrent.duration._
import scala.concurrent.{Future, Promise}

trait CellId {
  def getCellId: Long
}

trait TSOProtocol extends Closeable {

  def getEpoch: Option[Long]

  def getNewStartTimestamp: Future[Long]

  def commit(txId: Long, writeSet: Set[_ <: CellId]): Future[Long]

}

class TSOClient(val tsoClientConf: OmidClientConfiguration) extends TSOProtocol {

  val logger = Logger(classOf[TSOClient])

  val hp : HostAndPort = HostAndPort.fromString(tsoClientConf.getConnectionString)
  val tsoAddr = new InetSocketAddress(hp.getHostText, hp.getPort)
  logger.info("\t* TSO Client will connect to host:port {} directly", hp)

  val system : ActorSystem = ActorSystem.create("tso-client-actor-system");
  val fsm = system.actorOf(Props(classOf[TSOClientFSM], tsoAddr))

  val requestRetries = tsoClientConf.getRequestMaxRetries
  val requestTimeoutInMillis = Duration(tsoClientConf.getRequestTimeoutInMs, "millis")

  // ----------------------------------------------------------------------------------------------------------------
  // TSOProtocol interface
  // ----------------------------------------------------------------------------------------------------------------

  override def getEpoch = ???

  override def getNewStartTimestamp: Future[Long] = {

    val requestBuilder = TSOProto.Request.newBuilder
    requestBuilder.setTimestampRequest(TSOProto.TimestampRequest.newBuilder.build)
    val p = Promise[Long]()
    fsm ! Request(RequestData(requestBuilder.build, requestRetries, requestTimeoutInMillis, p))
    p.future

  }

  override def commit(txId: Long, writeSet: Set[_ <: CellId]): Future[Long] = {

    val commitRequestBuilder = TSOProto.CommitRequest.newBuilder
    commitRequestBuilder.setStartTimestamp(txId)
    writeSet.map(cell => commitRequestBuilder.addCellId(cell.getCellId))
    val requestBuilder = TSOProto.Request.newBuilder
    requestBuilder.setCommitRequest(commitRequestBuilder.build)
    val p = Promise[Long]()
    fsm ! Request(RequestData(requestBuilder.build, requestRetries, requestTimeoutInMillis, p))
    p.future

  }

  override def close(): Unit = {

    system.stop(fsm)

  }

}
