/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.admin

import java.io.PrintStream
import java.io.IOException
import java.util.Properties
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{ConcurrentLinkedQueue, TimeUnit}
import kafka.utils.Implicits._
import kafka.utils.Logging
import org.apache.kafka.common.utils.Utils
import org.apache.kafka.clients.{ApiVersions, ClientDnsLookup, ClientResponse, ClientUtils, CommonClientConfigs, Metadata, NetworkClient, NodeApiVersions}
import org.apache.kafka.clients.consumer.internals.{ConsumerNetworkClient, RequestFuture}
import org.apache.kafka.common.config.ConfigDef.ValidString._
import org.apache.kafka.common.config.ConfigDef.{Importance, Type}
import org.apache.kafka.common.config.{AbstractConfig, ConfigDef}
import org.apache.kafka.common.errors.AuthenticationException
import org.apache.kafka.common.internals.ClusterResourceListeners
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.network.Selector
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.utils.LogContext
import org.apache.kafka.common.utils.{KafkaThread, Time}
import org.apache.kafka.common.Node
import org.apache.kafka.common.requests.{AbstractRequest, AbstractResponse, ApiVersionsRequest, ApiVersionsResponse, MetadataRequest, MetadataResponse}
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.apache.kafka.server.util.{CommandDefaultOptions, CommandLineUtils}

import scala.jdk.CollectionConverters._
import scala.util.{Failure, Success, Try}

/**
 * A command for retrieving broker version information.
 */
object BrokerApiVersionsCommand {

  def main(args: Array[String]): Unit = {
    execute(args, System.out)
  }

  def execute(args: Array[String], out: PrintStream): Unit = {
    val opts = new BrokerVersionCommandOptions(args)
    val adminClient = createAdminClient(opts)
    adminClient.awaitBrokers()
    val brokerMap = adminClient.listAllBrokerVersionInfo()
    brokerMap.forKeyValue { (broker, versionInfoOrError) =>
      versionInfoOrError match {
        case Success(v) => out.print(s"${broker} -> ${v.toString(true)}\n")
        case Failure(v) => out.print(s"${broker} -> ERROR: ${v}\n")
      }
    }
    adminClient.close()
  }

  private def createAdminClient(opts: BrokerVersionCommandOptions): AdminClient = {
    val props = if (opts.options.has(opts.commandConfigOpt))
      Utils.loadProps(opts.options.valueOf(opts.commandConfigOpt))
    else
      new Properties()
    props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, opts.options.valueOf(opts.bootstrapServerOpt))
    AdminClient.create(props)
  }

  class BrokerVersionCommandOptions(args: Array[String]) extends CommandDefaultOptions(args) {
    val BootstrapServerDoc = "REQUIRED: The server to connect to."
    val CommandConfigDoc = "A property file containing configs to be passed to Admin Client."

    val commandConfigOpt = parser.accepts("command-config", CommandConfigDoc)
                                 .withRequiredArg
                                 .describedAs("command config property file")
                                 .ofType(classOf[String])
    val bootstrapServerOpt = parser.accepts("bootstrap-server", BootstrapServerDoc)
                                   .withRequiredArg
                                   .describedAs("server(s) to use for bootstrapping")
                                   .ofType(classOf[String])
    options = parser.parse(args : _*)
    checkArgs()

    def checkArgs(): Unit = {
      CommandLineUtils.maybePrintHelpOrVersion(this, "This tool helps to retrieve broker version information.")
      // check required args
      CommandLineUtils.checkRequiredArgs(parser, options, bootstrapServerOpt)
    }
  }

  // org.apache.kafka.clients.admin.AdminClient doesn't currently expose a way to retrieve the supported api versions.
  // We inline the bits we need from kafka.admin.AdminClient so that we can delete it.
  private class AdminClient(val time: Time,
                            val client: ConsumerNetworkClient,
                            val bootstrapBrokers: List[Node]) extends Logging {

    @volatile var running = true
    val pendingFutures = new ConcurrentLinkedQueue[RequestFuture[ClientResponse]]()

    val networkThread = new KafkaThread("admin-client-network-thread", () => {
      try {
        while (running)
          client.poll(time.timer(Long.MaxValue))
      } catch {
        case t: Throwable =>
          error("admin-client-network-thread exited", t)
      } finally {
        pendingFutures.forEach { future =>
          try {
            future.raise(Errors.UNKNOWN_SERVER_ERROR)
          } catch {
            case _: IllegalStateException => // It is OK if the future has been completed
          }
        }
        pendingFutures.clear()
      }
    }, true)

    networkThread.start()

    private def send(target: Node,
                     request: AbstractRequest.Builder[_ <: AbstractRequest]): AbstractResponse = {
      val future = client.send(target, request)
      pendingFutures.add(future)
      future.awaitDone(Long.MaxValue, TimeUnit.MILLISECONDS)
      pendingFutures.remove(future)
      if (future.succeeded())
        future.value().responseBody()
      else
        throw future.exception()
    }

    private def sendAnyNode(request: AbstractRequest.Builder[_ <: AbstractRequest]): AbstractResponse = {
      bootstrapBrokers.foreach { broker =>
        try {
          return send(broker, request)
        } catch {
          case e: AuthenticationException =>
            throw e
          case e: Exception =>
            debug(s"Request ${request.apiKey()} failed against node $broker", e)
        }
      }
      throw new RuntimeException(s"Request ${request.apiKey()} failed on brokers $bootstrapBrokers")
    }

    private def getNodeApiVersions(node: Node): NodeApiVersions = {
      val response = send(node, new ApiVersionsRequest.Builder()).asInstanceOf[ApiVersionsResponse]
      Errors.forCode(response.data.errorCode).maybeThrow()
      new NodeApiVersions(response.data.apiKeys, response.data.supportedFeatures, response.data.zkMigrationReady)
    }

    /**
     * Wait until there is a non-empty list of brokers in the cluster.
     */
    def awaitBrokers(): Unit = {
      var nodes = List[Node]()
      do {
        nodes = findAllBrokers()
        if (nodes.isEmpty)
          Thread.sleep(50)
      } while (nodes.isEmpty)
    }

    private def findAllBrokers(): List[Node] = {
      val request = MetadataRequest.Builder.allTopics()
      val response = sendAnyNode(request).asInstanceOf[MetadataResponse]
      val errors = response.errors
      if (!errors.isEmpty)
        debug(s"Metadata request contained errors: $errors")
      response.buildCluster.nodes.asScala.toList
    }

    def listAllBrokerVersionInfo(): Map[Node, Try[NodeApiVersions]] =
      findAllBrokers().map { broker =>
        broker -> Try[NodeApiVersions](getNodeApiVersions(broker))
      }.toMap

    def close(): Unit = {
      running = false
      try {
        client.close()
      } catch {
        case e: IOException =>
          error("Exception closing nioSelector:", e)
      }
    }

  }

  private object AdminClient {
    val DefaultConnectionMaxIdleMs = 9 * 60 * 1000
    val DefaultRequestTimeoutMs = 5000
    val DefaultMaxInFlightRequestsPerConnection = 100
    val DefaultReconnectBackoffMs = 50
    val DefaultReconnectBackoffMax = 50
    val DefaultSendBufferBytes = 128 * 1024
    val DefaultReceiveBufferBytes = 32 * 1024
    val DefaultRetryBackoffMs = 100

    val AdminClientIdSequence = new AtomicInteger(1)
    val AdminConfigDef = {
      val config = new ConfigDef()
        .define(
          CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG,
          Type.LIST,
          Importance.HIGH,
          CommonClientConfigs.BOOTSTRAP_SERVERS_DOC)
        .define(CommonClientConfigs.CLIENT_DNS_LOOKUP_CONFIG,
          Type.STRING,
          ClientDnsLookup.USE_ALL_DNS_IPS.toString,
          in(ClientDnsLookup.USE_ALL_DNS_IPS.toString,
            ClientDnsLookup.RESOLVE_CANONICAL_BOOTSTRAP_SERVERS_ONLY.toString),
          Importance.MEDIUM,
          CommonClientConfigs.CLIENT_DNS_LOOKUP_DOC)
        .define(
          CommonClientConfigs.SECURITY_PROTOCOL_CONFIG,
          ConfigDef.Type.STRING,
          CommonClientConfigs.DEFAULT_SECURITY_PROTOCOL,
          ConfigDef.CaseInsensitiveValidString.in(Utils.enumOptions(classOf[SecurityProtocol]):_*),
          ConfigDef.Importance.MEDIUM,
          CommonClientConfigs.SECURITY_PROTOCOL_DOC)
        .define(
          CommonClientConfigs.REQUEST_TIMEOUT_MS_CONFIG,
          ConfigDef.Type.INT,
          DefaultRequestTimeoutMs,
          ConfigDef.Importance.MEDIUM,
          CommonClientConfigs.REQUEST_TIMEOUT_MS_DOC)
        .define(
          CommonClientConfigs.SOCKET_CONNECTION_SETUP_TIMEOUT_MS_CONFIG,
          ConfigDef.Type.LONG,
          CommonClientConfigs.DEFAULT_SOCKET_CONNECTION_SETUP_TIMEOUT_MS,
          ConfigDef.Importance.MEDIUM,
          CommonClientConfigs.SOCKET_CONNECTION_SETUP_TIMEOUT_MS_DOC)
        .define(
          CommonClientConfigs.SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS_CONFIG,
          ConfigDef.Type.LONG,
          CommonClientConfigs.DEFAULT_SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS,
          ConfigDef.Importance.MEDIUM,
          CommonClientConfigs.SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS_DOC)
        .define(
          CommonClientConfigs.RETRY_BACKOFF_MS_CONFIG,
          ConfigDef.Type.LONG,
          DefaultRetryBackoffMs,
          ConfigDef.Importance.MEDIUM,
          CommonClientConfigs.RETRY_BACKOFF_MS_DOC)
        .withClientSslSupport()
        .withClientSaslSupport()
      config
    }

    class AdminConfig(originals: Map[_,_]) extends AbstractConfig(AdminConfigDef, originals.asJava, false)

    def create(props: Properties): AdminClient = create(props.asScala.toMap)

    def create(props: Map[String, _]): AdminClient = create(new AdminConfig(props))

    def create(config: AdminConfig): AdminClient = {
      val clientId = "admin-" + AdminClientIdSequence.getAndIncrement()
      val logContext = new LogContext(s"[LegacyAdminClient clientId=$clientId] ")
      val time = Time.SYSTEM
      val metrics = new Metrics(time)
      val metadata = new Metadata(CommonClientConfigs.DEFAULT_RETRY_BACKOFF_MS,
        CommonClientConfigs.DEFAULT_RETRY_BACKOFF_MAX_MS,
        60 * 60 * 1000L, logContext,
        new ClusterResourceListeners)
      val channelBuilder = ClientUtils.createChannelBuilder(config, time, logContext)
      val requestTimeoutMs = config.getInt(CommonClientConfigs.REQUEST_TIMEOUT_MS_CONFIG)
      val connectionSetupTimeoutMs = config.getLong(CommonClientConfigs.SOCKET_CONNECTION_SETUP_TIMEOUT_MS_CONFIG)
      val connectionSetupTimeoutMaxMs = config.getLong(CommonClientConfigs.SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS_CONFIG)
      val retryBackoffMs = config.getLong(CommonClientConfigs.RETRY_BACKOFF_MS_CONFIG)

      val brokerUrls = config.getList(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG)
      val clientDnsLookup = config.getString(CommonClientConfigs.CLIENT_DNS_LOOKUP_CONFIG)
      val brokerAddresses = ClientUtils.parseAndValidateAddresses(brokerUrls, clientDnsLookup)
      metadata.bootstrap(brokerAddresses)

      val selector = new Selector(
        DefaultConnectionMaxIdleMs,
        metrics,
        time,
        "admin",
        channelBuilder,
        logContext)

      val networkClient = new NetworkClient(
        selector,
        metadata,
        clientId,
        DefaultMaxInFlightRequestsPerConnection,
        DefaultReconnectBackoffMs,
        DefaultReconnectBackoffMax,
        DefaultSendBufferBytes,
        DefaultReceiveBufferBytes,
        requestTimeoutMs,
        connectionSetupTimeoutMs,
        connectionSetupTimeoutMaxMs,
        time,
        true,
        new ApiVersions,
        logContext)

      val highLevelClient = new ConsumerNetworkClient(
        logContext,
        networkClient,
        metadata,
        time,
        retryBackoffMs,
        requestTimeoutMs,
        Integer.MAX_VALUE)

      new AdminClient(
        time,
        highLevelClient,
        metadata.fetch.nodes.asScala.toList)
    }
  }

}
