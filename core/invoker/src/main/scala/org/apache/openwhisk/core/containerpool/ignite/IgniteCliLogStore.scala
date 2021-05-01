package org.apache.openwhisk.core.containerpool.ignite


import java.time.Instant
import akka.actor.ActorSystem
import org.apache.openwhisk.common.{AkkaLogging, Logging, TransactionId}
import org.apache.openwhisk.core.containerpool.Container.ACTIVATION_LOG_SENTINEL
import org.apache.openwhisk.core.containerpool.logging.{DockerToActivationLogStore, LogStore, LogStoreProvider}
import org.apache.openwhisk.core.containerpool.{Container, ContainerId}
import org.apache.openwhisk.core.entity.{ActivationLogs, ExecutableWhiskAction, Identity, WhiskActivation}

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}

/**
 * Docker based log store implementation which fetches logs via cli command.
 * This mode is inefficient and is only provided for running in developer modes
 */
object IgniteCliLogStoreProvider extends LogStoreProvider {
  override def instance(actorSystem: ActorSystem): LogStore = {
    //Logger is currently not passed implicitly to LogStoreProvider. So create one explicitly
    implicit val logger = new AkkaLogging(akka.event.Logging.getLogger(actorSystem, this))
    new IgniteCliLogStore(actorSystem)
  }
}

class IgniteCliLogStore(system: ActorSystem)(implicit log: Logging) extends DockerToActivationLogStore(system) {
  private val client = new ExtendedIgniteClient()(system.dispatcher)(log, system)
  override def collectLogs(transid: TransactionId,
                           user: Identity,
                           activation: WhiskActivation,
                           container: Container,
                           action: ExecutableWhiskAction): Future[ActivationLogs] = {
    new ExtendedIgniteClient()(system.dispatcher)(log, system)
      .collectLogs(container.containerId, activation.start, activation.end)(transid)
      .map(logs => ActivationLogs(logs.linesIterator.takeWhile(!_.contains(ACTIVATION_LOG_SENTINEL)).toVector))
  }
}

class ExtendedIgniteClient()(executionContext: ExecutionContext)(implicit log: Logging, as: ActorSystem)
  extends IgniteClient()(executionContext,as,log)
    {
  implicit private val ec: ExecutionContext = executionContext
  private val waitForLogs: FiniteDuration = 2.seconds
  private val logTimeSpanMargin = 1.second

  def collectLogs(id: ContainerId, since: Instant, untill: Instant)(implicit transid: TransactionId): Future[String] = {
    //Add a slight buffer to account for delay writes of logs
    val end = untill.plusSeconds(logTimeSpanMargin.toSeconds)
    val logs=runCmd(
      Seq(
        "logs",
        id.asString),
      waitForLogs)

    println(s"igniteCliLogStore collectLogs: \n$logs")
    logs
  }

}
