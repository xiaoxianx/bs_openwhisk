package org.apache.openwhisk.core.containerpool.ignite

import akka.actor.ActorSystem
import org.apache.openwhisk.common.{Logging, TransactionId}
import org.apache.openwhisk.core.{ConfigKeys, WhiskConfig}
import org.apache.openwhisk.core.containerpool.{Container, ContainerFactory, ContainerFactoryProvider}
import org.apache.openwhisk.core.entity.{ByteSize, ExecManifest, InvokerInstanceId}
import pureconfig._
import pureconfig.generic.auto._

import java.util.concurrent.TimeoutException
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration._

object IgniteContainerFactoryProvider extends ContainerFactoryProvider {
  override def instance(actorSystem: ActorSystem,
                        logging: Logging,
                        config: WhiskConfig,
                        instanceId: InvokerInstanceId,
                        parameters: Map[String, Set[String]]): ContainerFactory = {
    //Ignore parameters as they are  specific for Docker based creation. They do not map to Ignite

    new IgniteContainerFactory(instanceId)(
      actorSystem,
      actorSystem.dispatcher,
      logging,
      new IgniteClient()(actorSystem.dispatcher, actorSystem, logging))
  }
}

case class IgniteConfig(extraArgs: Map[String, Set[String]],defaultRunResource: Map[String,String])



class IgniteContainerFactory(instance: InvokerInstanceId)(implicit actorSystem: ActorSystem,
                                                          ec: ExecutionContext,
                                                          logging: Logging,
                                                          ignite: IgniteClientApi,
                                                          igniteConfig: IgniteConfig =
                                                          loadConfigOrThrow[IgniteConfig](ConfigKeys.ignite)
)
  extends ContainerFactory {

  override def createContainer(tid: TransactionId,
                               name: String,
                               actionImage: ExecManifest.ImageName,
                               userProvidedImage: Boolean,
                               memory: ByteSize,
                               cpuShares: Int)(implicit config: WhiskConfig, logging: Logging): Future[Container] = {
    IgniteContainer.create(tid, actionImage, memory, cpuShares, Some(name))
  }

  override def init(): Unit = {
    println("IgniteContainerFactory init remove all")
    val rem= removeAllActionContainers()
    println("IgniteContainerFactory init remove all   invokerReactive containerFactory.init finish")
  }




  override def cleanup(): Unit = {
    try {
      removeAllActionContainers()
    } catch {
      case e: Exception => logging.error(this, s"Failed to remove action containers: ${e.getMessage}")
    }
  }

  @throws(classOf[TimeoutException])
  @throws(classOf[InterruptedException])
  private def removeAllActionContainers()  = {
    implicit val transid = TransactionId.invoker
    val cleaning =
      ignite.ps(filters = Seq("{{.ObjectMeta.Name}}" -> s"~${ContainerFactory.containerNamePrefix(instance)}"), all = true).flatMap {
        containers =>
          logging.info(this, s"removing ${containers.size} action containers.")
          val removals = containers.map { id =>
            ignite.stopAndRemove(id)

          }
          Future.sequence(removals)
      }
    Await.ready(cleaning, 30.seconds)
  }
}