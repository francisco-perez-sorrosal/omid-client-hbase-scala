package com.fps.omid.client.hbase

import java.net.{SocketException, UnknownHostException}
import javax.inject.Singleton

import com.google.inject.{AbstractModule, Inject, Provider, Provides}
import com.typesafe.scalalogging.Logger
import org.apache.omid.committable.{CommitTable, InMemoryCommitTable}
import org.apache.omid.metrics.{MetricsRegistry, NullMetricsProvider}
import org.apache.omid.timestamp.storage.TimestampStorage
import org.apache.omid.tso._

class TSOMockModule(val config: TSOServerConfig) extends AbstractModule {

  protected def configure() {
    bind(classOf[TSOChannelHandler]).in(classOf[Singleton])
    bind(classOf[TSOStateManager]).to(classOf[TSOStateManagerImpl]).in(classOf[Singleton])
    bind(classOf[CommitTable]).to(classOf[InMemoryCommitTable]).in(classOf[Singleton])
    bind(classOf[TimestampStorage]).to(classOf[InMemoryTimestampStorage]).in(classOf[Singleton])
    bind(classOf[TimestampOracle]).to(classOf[PausableTimestampOracle]).in(classOf[Singleton])
    bind(classOf[Panicker]).to(classOf[MockPanicker]).in(classOf[Singleton])
    install(new BatchPoolModule(config))
    install(config.getLeaseModule)
    install(new DisruptorModule())
  }

  @Provides
  def provideTSOServerConfig = {
    config
  }

  @Provides
  @Singleton
  def provideMetricsRegistry: MetricsRegistry = {
    new NullMetricsProvider
  }

  @Provides
  @throws[SocketException]
  @throws[UnknownHostException]
  def provideTSOHostAndPort = {
    NetworkInterfaceUtils.getTSOHostAndPort(config)
  }

  @Provides
  def getPersistenceProcessorHandler(provider: Provider[PersistenceProcessorHandler]) = {

    (for (i <- 1 to config.getNumConcurrentCTWriters) yield provider.get()).toArray

  }

}

class PausableTimestampOracle @Inject()(val metrics: MetricsRegistry,
                                        val tsStorage: TimestampStorage,
                                        val panicker: Panicker)
  extends TimestampOracleImpl(metrics, tsStorage, panicker) {

  val logger = Logger(classOf[PausableTimestampOracle])

  private var tsoPaused = false

  override def next: Long = {
    while (tsoPaused) this synchronized {
      try
        this.wait()

      catch {
        case e: InterruptedException => {
          logger.error("Interrupted whilst paused")
          Thread.currentThread.interrupt()
        }
      }
    }
    super.next
  }

  def pause() {
    tsoPaused = true
    this.notifyAll()
  }

  def resume() {
    tsoPaused = false
    this.notifyAll()
  }

  def isTSOPaused: Boolean = tsoPaused

}

class InMemoryTimestampStorage extends TimestampStorage {

  val logger = Logger(classOf[InMemoryTimestampStorage])

  private var maxTimestamp: Long = 0

  def updateMaxTimestamp(previousMaxTimestamp: Long, nextMaxTimestamp: Long) {
    maxTimestamp = nextMaxTimestamp
    logger.info("Updating max timestamp: (new:{})", nextMaxTimestamp)
  }

  def getMaxTimestamp: Long = maxTimestamp

}