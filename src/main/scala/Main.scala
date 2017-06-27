package com.fps.omid.client.hbase


import com.typesafe.scalalogging.Logger
import org.apache.omid.tso.client.OmidClientConfiguration

import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.util.{Failure, Success}



object SimpleTransaction extends App {

  val logger = Logger("SimpleTransaction")

  logger.info("Starting TSO client test!")

  val conf = new OmidClientConfiguration

  logger.info(conf.toString)

  val client = new TSOClient(conf)

  while(true) {
    logger.info("------------------------------------------------------------------------------------------------------")

    val f = client.getNewStartTimestamp
    f onComplete {
      case Success(ts) => {
        logger.info("Timestamp Received {}", ts)
        val cf = client.commit(ts, Set.empty)
        cf onComplete {
          case Success(cts) =>
            logger.info("Commit Timestamp Received {}", cts)
          case Failure(t) =>
            logger.error("An error has occured: {}", t.getMessage)
        }
      }
      case Failure(t) => logger.error("An error has occured: {}", t.getMessage)
    }

    Thread sleep 100

  }

}
