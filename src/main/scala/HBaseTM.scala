package com.fps.omid.client.hbase

import org.apache.omid.committable.CommitTable
import org.apache.omid.metrics.MetricsRegistry
import org.apache.omid.transaction.{PostCommitActions, AbstractTransactionManager => TM}
import org.apache.omid.tso.client.{CellId, TSOClient}


///**
//  * Created by fperez on 6/13/17.
//  */
//class BasicTM(val metrics: MetricsRegistry,
//              val postCommitter: PostCommitActions,
//              val tsoClient: TSOClient,
//              val commitTableClient: CommitTable.Client,
//              val abstractTransactionFactory: TM.TransactionFactory[_ <: CellId])
//
//  extends TM(metrics, postCommitter, tsoClient, commitTableClient, abstractTransactionFactory) {
//
//}

