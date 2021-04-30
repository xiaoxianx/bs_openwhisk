package org.apache.openwhisk.core.containerpool.ignite

import java.time.Instant
import akka.actor.ActorSystem
import akka.stream.scaladsl.Source
import akka.util.ByteString
import org.apache.openwhisk.common.{Logging, TransactionId}
import org.apache.openwhisk.core.containerpool.logging.LogLine
import org.apache.openwhisk.core.containerpool.{BlackboxStartupError, Container, ContainerAddress, ContainerId, WhiskContainerStartupError}
import org.apache.openwhisk.core.entity.ByteSize
import org.apache.openwhisk.core.entity.ExecManifest.ImageName
import org.apache.openwhisk.core.entity.size._
import org.apache.openwhisk.http.Messages
import spray.json._

import scala.concurrent.{ExecutionContext, Future}

case class IgniteId(asString: String) {
  require(asString.nonEmpty, "IgniteId must not be empty")
}

object IgniteContainer {

  def create(transid: TransactionId,
             image: ImageName,
             memory: ByteSize = 256.MB,
             cpuShares: Int = 0,
             name: Option[String] = None)(implicit
                                          as: ActorSystem,
                                          ec: ExecutionContext,
                                          log: Logging,
                                          config: IgniteConfig,
                                          ignite: IgniteClientApi): Future[IgniteContainer] = {
    implicit val tid: TransactionId = transid
    val params = config.extraArgs.flatMap {
      case (key, valueList) => valueList.toList.flatMap(Seq(key, _))
    }
    //TODO Environment handling

    //TODO cpus - VM vCPU count, 1 or even numbers between 1 and 32 (default 1)
    //It does not map to cpuShares currently. We may use it proportionally

    //size - VM filesystem size, for example 5GB or 2048MB (default 4.0 GB)
    val args : Seq[String]= Seq("--cpus", config.defaultRunResource("cpu"), "--memory", s"${memory.toMB}m", "--size",  config.defaultRunResource("size")) ++ name
      .map(n => Seq("--name", n))
      .getOrElse(Seq.empty) ++ params
    //TODO
    val imageToUse =image.resolveImageName()   // image.publicImageName
    for {
      importSuccessful <- ignite.importImage(imageToUse)
      igniteId <- ignite.run(imageToUse, args).recoverWith {
        case _ =>
          if (importSuccessful) {
            Future.failed(WhiskContainerStartupError(Messages.resourceProvisionError))
          } else {
            Future.failed(BlackboxStartupError(Messages.imagePullError(imageToUse)))
          }
      }
      containerId <- ignite.containerId(igniteId)
      ip <- ignite.inspectIPAddress(containerId).recoverWith {
        // remove the container immediately if inspect failed as
        // we cannot recover that case automatically
        case _ =>
          ignite.rm(igniteId)
          Future.failed(WhiskContainerStartupError(Messages.resourceProvisionError))
      }
    } yield new IgniteContainer(containerId, ip, igniteId)
  }

}

class IgniteContainer(protected[core] val id: ContainerId, protected[core] val addr: ContainerAddress, igniteId: IgniteId)(
  implicit
  override protected val as: ActorSystem,
  protected val ec: ExecutionContext,
  protected val logging: Logging,
  ignite: IgniteClientApi)
  extends Container {

  override def destroy()(implicit transid: TransactionId): Future[Unit] = {
    super.destroy()
    ignite.stopAndRemove(igniteId)
  }

  private val logMsg = "LogMessage are collected via Docker CLI"
  override def logs(limit: ByteSize, waitForSentinel: Boolean)(
    implicit transid: TransactionId): Source[ByteString, Any] =
    Source.single(ByteString(LogLine(logMsg, "stdout", Instant.now.toString).toJson.compactPrint))

  override def toString() = s"igniteId: ${igniteId.asString}, docker: ${id.asString}, address: $addr"
}