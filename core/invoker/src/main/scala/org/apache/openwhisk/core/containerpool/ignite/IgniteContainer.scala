package org.apache.openwhisk.core.containerpool.ignite

import java.time.Instant
import akka.actor.ActorSystem
import akka.stream.scaladsl.Source
import akka.util.ByteString
import org.apache.openwhisk.common.{Logging, TransactionId}
import org.apache.openwhisk.core.containerpool.logging.LogLine
import org.apache.openwhisk.core.containerpool.{AkkaContainerClient, ApacheBlockingContainerClient, BlackboxStartupError, Container, ContainerAddress, ContainerId, Interval, RunResult, WhiskContainerStartupError}
import org.apache.openwhisk.core.entity.ActivationResponse.{ConnectionError, MemoryExhausted}
import org.apache.openwhisk.core.entity.{ActivationEntityLimit, ByteSize}
import org.apache.openwhisk.core.entity.ExecManifest.ImageName
import org.apache.openwhisk.core.entity.size._
import org.apache.openwhisk.http.Messages
import spray.json._

import java.util.concurrent.atomic.AtomicLong
import scala.concurrent.duration.{DurationInt, FiniteDuration}
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

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
    //cpus - VM vCPU count, 1 or even numbers between 1 and 32 (default 1)
    //It does not map to cpuShares currently. We may use it proportionally
    //size - VM filesystem size, for example 5GB or 2048MB (default 4.0 GB)

    val args : Seq[String]= Seq("--cpus", config.defaultRunResource("cpu"), "--memory", s"${memory.toMB}m", "--size",  config.defaultRunResource("size")) ++ name
      .map(n => Seq("--name", n))
      .getOrElse(Seq.empty) ++ params
    //TODO  only support js
    // val imageToUse = image.merge.resolveImageName(Some(registryConfigUrl))
   // val imageToUse =image.resolveImageName()
   // val imageToUse="whisk/ignite-nodejs-v12:latest"
    val imageToUse="whisk/ignite-ubuntu-nodejs-v12:latest"
    //val imageToUse=
    println(s"ignite container creat  imageToUse: $imageToUse   imageName: ${image.name}    *${image.prefix}*${image.registry}*${image.tag}")
    // imageName: action-nodejs-v10    *Some(openwhisk)*None*Some(nightly)



    // // 这里之前 shh deamon没启动 就返回  igniteId了
    //  或者启动了 -q 还是多数出信息，要解析出 id
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

      ip <- {
        var flag=0
        var a : ContainerAddress=null
        while(flag<10){
          Try(Await.result(ignite.inspectIPAddress(igniteId),5.seconds)) match {
            case Success(res) => {
              flag=100
              a=res
            }
            case Failure(e) => {
              flag+=1
              println(s"start ssh deamon  $flag st try again")
              Thread.sleep(2000)
            }
          }
        }
        if (a==null) {
          ignite.rm(igniteId)
          Future.failed(WhiskContainerStartupError(Messages.resourceProvisionError))
        } else {
          Future.successful(a)
        }
      }
/*      ip <- ignite.inspectIPAddress(igniteId).recoverWith {
        // remove the container immediately if inspect failed as
        // we cannot recover that case automatically
        case _ =>
          ignite.rm(igniteId)
          Future.failed(WhiskContainerStartupError(Messages.resourceProvisionError))
      }*/

    } yield new IgniteContainer(ip, igniteId)
  }

}

class IgniteContainer(protected[core] val addr: ContainerAddress, igniteId: IgniteId)(
  implicit
  override protected val as: ActorSystem,
  protected val ec: ExecutionContext,
  protected val logging: Logging,
  ignite: IgniteClientApi)
  extends Container {

  // 都命名contaier id 就不用这么了
  override protected val id: ContainerId = ContainerId.apply(igniteId.asString)

  override def destroy()(implicit transid: TransactionId): Future[Unit] = {
    super.destroy()
    ignite.stopAndRemove(igniteId)
  }
  def idAndAddrString() = s"igniteId: ${igniteId.asString}, address: $addr"



// todo    isoomkilled  call  fetch

  private var logFileOffset = new AtomicLong(0)
  protected val logCollectingIdleTimeout: FiniteDuration = 2.seconds
  protected val logCollectingTimeoutPerMBLogLimit: FiniteDuration = 2.seconds
  protected val waitForOomState: FiniteDuration = 2.seconds
  protected val filePollInterval: FiniteDuration = 5.milliseconds
  /** Delimiter used to split log-lines as written by the json-log-driver. */
  private val delimiter = ByteString("\n")
  private val logMsg = "LogMessage are collected via Docker CLI"
  override def logs(limit: ByteSize, waitForSentinel: Boolean)(
    implicit transid: TransactionId): Source[ByteString, Any] =
    Source.single(ByteString(LogLine(logMsg, "stdout", `Instant`.now.toString).toJson.compactPrint))


  private def isOomKilled(retries: Int = (waitForOomState / filePollInterval).toInt)(
    implicit transid: TransactionId): Future[Boolean] = {
/*    docker.isOomKilled(id)(TransactionId.invoker).flatMap { killed =>
      if (killed) Future.successful(true)
      else if (retries > 0) akka.pattern.after(filePollInterval, as.scheduler)(isOomKilled(retries - 1))
      else Future.successful(false)
    }*/
    Future.successful(false)
  }

  override protected def  callContainer(
                                        path: String,
                                        body: JsObject,
                                        timeout: FiniteDuration,
                                        maxConcurrent: Int,
                                        retry: Boolean = false,
                                        reschedule: Boolean = false)(implicit transid: TransactionId): Future[RunResult] = {

    println(s"Call Container\n  path: $path \n body $body ")

    val started = Instant.now()
    val http = httpConnection.getOrElse {
      val conn = if (Container.config.akkaClient) {
        new AkkaContainerClient(
          addr.host,
          addr.port,
          timeout,
          ActivationEntityLimit.MAX_ACTIVATION_ENTITY_LIMIT,
          ActivationEntityLimit.MAX_ACTIVATION_ENTITY_TRUNCATION_LIMIT,
          1024)
      } else {
        new ApacheBlockingContainerClient(
          s"${addr.host}:${addr.port}",
          timeout,
          ActivationEntityLimit.MAX_ACTIVATION_ENTITY_LIMIT,
          ActivationEntityLimit.MAX_ACTIVATION_ENTITY_TRUNCATION_LIMIT,
          maxConcurrent)
      }
      httpConnection = Some(conn)
      conn
    }

    http
      .post(path, body, retry, reschedule)
      .flatMap { response =>
        val finished = Instant.now()

        response.left
          .map {
            // Only check for memory exhaustion if there was a
            // terminal connection error.
            case error: ConnectionError =>
              isOomKilled().map {
                case true  => MemoryExhausted()
                case false => error
              }
            case other => Future.successful(other)
          }
          .fold(_.map(Left(_)), right => Future.successful(Right(right)))
          .map(res => RunResult(Interval(started, finished), res))
      }
  }


  override def getMemInfo()(implicit transid: TransactionId): Future[String] = {
    ignite.getMemInfo(igniteId)
  }

  override def getCpuInfo()(implicit transid: TransactionId): Future[String] = {
    ignite.getCpuInfo(igniteId)
  }


  override def changeMemory(memBlockSize: Int)(implicit transid: TransactionId): Future[Unit] = {
    //todo changeMemory
    Future.successful(():Unit)
  }

  override def changeCpus(cpus: Int)(implicit transid: TransactionId): Future[Unit] = {
    //todo changeCpus
    Future.successful(():Unit)
  }

}


/*
  ignite run <OCI image> [flags]
  ignite run args

      --config string                     Specify a path to a file with the API resources you want to pass
  -f, --copy-files strings                Copy files/directories from the host to the created VM
      --cpus uint                         VM vCPU count, 1 or even numbers between 1 and 32 (default 1)
  -d, --debug                             Debug mode, keep container after VM shutdown
  -h, --help                              help for run
      --id-prefix string                  Prefix string for system identifiers (default ignite)
      --ignore-preflight-checks strings   A list of checks whose errors will be shown as warnings. Example: 'BinaryInPath,Port,ExistingFile'. Value 'all' ignores errors from all checks.
  -i, --interactive                       Attach to the VM after starting
      --kernel-args string                Set the command line for the kernel (default "console=ttyS0 reboot=k panic=1 pci=off ip=dhcp")
  -k, --kernel-image oci-image            Specify an OCI image containing the kernel at /boot/vmlinux and optionally, modules (default weaveworks/ignite-kernel:5.4.108)
  -l, --label stringArray                 Set a label (foo=bar)
      --memory size                       Amount of RAM to allocate for the VM (default 512.0 MB)
  -n, --name string                       Specify the name
      --network-plugin plugin             Network plugin to use. Available options are: [cni docker-bridge] (default cni)
  -p, --ports strings                     Map host ports to VM ports
      --require-name                      Require VM name to be passed, no name generation
      --runtime runtime                   Container runtime to use. Available options are: [docker containerd] (default containerd)
      --sandbox-image oci-image           Specify an OCI image for the VM sandbox (default weaveworks/ignite:dev)
  -s, --size size                         VM filesystem size, for example 5GB or 2048MB (default 4.0 GB)
      --ssh[=<path>]                      Enable SSH for the VM. If <path> is given, it will be imported as the public key. If just '--ssh' is specified, a new keypair will be generated. (default is unset, which disables SSH access to the VM)
  -v, --volumes volume                    Expose block devices from the host inside the VM
 */