/**
  * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
  * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
  * to You under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
  * License. You may obtain a copy of the License at
  *
  * http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
  * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
  * specific language governing permissions and limitations under the License.
  */
package kafka.api

import java.util
import java.util.concurrent._
import java.util.Properties
import com.yammer.metrics.core.Gauge
import kafka.security.JaasTestUtils
import kafka.utils.TestUtils
import org.apache.kafka.clients.admin.{AdminClientConfig, CreateAclsResult, DescribeClusterOptions}
import org.apache.kafka.common.acl._
import org.apache.kafka.common.config.SslConfigs
import org.apache.kafka.common.config.internals.BrokerSecurityConfigs
import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.resource.{PatternType, ResourcePattern, ResourceType}
import org.apache.kafka.common.security.auth.{AuthenticationContext, KafkaPrincipal, SecurityProtocol, SslAuthenticationContext}
import org.apache.kafka.common.security.authenticator.DefaultKafkaPrincipalBuilder
import org.apache.kafka.server.authorizer._
import org.apache.kafka.common.network.ConnectionMode
import org.apache.kafka.common.utils.Utils
import org.apache.kafka.metadata.authorizer.{ClusterMetadataAuthorizer, StandardAuthorizer}
import org.apache.kafka.server.metrics.KafkaYammerMetrics
import org.junit.jupiter.api.Assertions.{assertEquals, assertFalse, assertNotNull, assertThrows, assertTrue}
import org.junit.jupiter.api.{AfterEach, TestInfo}
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource

import scala.jdk.CollectionConverters._
import scala.collection.{Seq, mutable}
import scala.jdk.javaapi.OptionConverters

object SslAdminIntegrationTest {
  @volatile var semaphore: Option[Semaphore] = None
  @volatile var executor: Option[ExecutorService] = None
  @volatile var lastUpdateRequestContext: Option[AuthorizableRequestContext] = None
  val superuserCn = "super-user"
  val serverUser = "server"
  val clientCn = "client"

  class TestableStandardAuthorizer extends StandardAuthorizer with ClusterMetadataAuthorizer {
    override def createAcls(requestContext: AuthorizableRequestContext,
                            aclBindings: util.List[AclBinding]): util.List[_ <: CompletionStage[AclCreateResult]] = {
      lastUpdateRequestContext = Some(requestContext)
      execute[AclCreateResult](aclBindings.size, () => super.createAcls(requestContext, aclBindings))
    }

    override def deleteAcls(requestContext: AuthorizableRequestContext,
                            aclBindingFilters: util.List[AclBindingFilter]): util.List[_ <: CompletionStage[AclDeleteResult]] = {
      lastUpdateRequestContext = Some(requestContext)
      execute[AclDeleteResult](aclBindingFilters.size, () => super.deleteAcls(requestContext, aclBindingFilters))
    }

    private def execute[T](batchSize: Int, action: () => util.List[_ <: CompletionStage[T]]): util.List[CompletableFuture[T]] = {
      val futures = (0 until batchSize).map(_ => new CompletableFuture[T]).toList
      val runnable = new Runnable {
        override def run(): Unit = {
          semaphore.foreach(_.acquire())
          try {
            action.apply().asScala.zip(futures).foreach { case (baseFuture, resultFuture) =>
              try {
                resultFuture.complete(baseFuture.toCompletableFuture.get())
              } catch  {
                case e: Throwable => resultFuture.completeExceptionally(e)
              }
            }
          } finally {
            semaphore.foreach(_.release())
          }
        }
      }
      executor match {
        case Some(executorService) => executorService.submit(runnable)
        case None => runnable.run()
      }
      futures.asJava
    }
  }

  class TestPrincipalBuilder extends DefaultKafkaPrincipalBuilder(null, null) {
    private val Pattern = "O=A (.*?),CN=(.*?)".r

    // Use fields from DN as principal to grant appropriate permissions
    override def build(context: AuthenticationContext): KafkaPrincipal = {
      if (context.securityProtocol().equals(SecurityProtocol.PLAINTEXT)) {
        KafkaPrincipal.ANONYMOUS
      } else {
        val peerPrincipal = context.asInstanceOf[SslAuthenticationContext].session.getPeerPrincipal.getName
        peerPrincipal match {
          case Pattern(name, cn) =>
            val principal =
              if ((name == serverUser) || (cn == superuserCn)) serverUser
              else if (cn == clientCn) clientCn
              else KafkaPrincipal.ANONYMOUS.getName
            new KafkaPrincipal(KafkaPrincipal.USER_TYPE, principal)
          case _ =>
            KafkaPrincipal.ANONYMOUS
        }
      }
    }
  }
}

class SslAdminIntegrationTest extends SaslSslAdminIntegrationTest {
  override val kraftAuthorizerClassName: String = classOf[SslAdminIntegrationTest.TestableStandardAuthorizer].getName

  this.serverConfig.setProperty(BrokerSecurityConfigs.SSL_CLIENT_AUTH_CONFIG, "required")
  this.serverConfig.setProperty(BrokerSecurityConfigs.PRINCIPAL_BUILDER_CLASS_CONFIG, classOf[SslAdminIntegrationTest.TestPrincipalBuilder].getName)
  override protected def securityProtocol = SecurityProtocol.SSL
  override val kafkaPrincipal = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, SslAdminIntegrationTest.serverUser)

  private val extraControllerSecurityProtocol = SecurityProtocol.SSL

  override def setUpSasl(): Unit = {
    SslAdminIntegrationTest.semaphore = None
    SslAdminIntegrationTest.executor = None
    SslAdminIntegrationTest.lastUpdateRequestContext = None

    startSasl(jaasSections(List.empty, None, KafkaSasl))
  }

  override def createConfig: util.Map[String, Object] = {
    val config = new util.HashMap[String, Object]
    config.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "20000")
    config.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, "40000")
    config
  }

  override def kraftControllerConfigs(testInfo: TestInfo): Seq[Properties] = {
    val configs = super.kraftControllerConfigs(testInfo)
    JaasTestUtils.sslConfigs(ConnectionMode.SERVER, false, OptionConverters.toJava(trustStoreFile), s"controller")
      .forEach((k, v) => configs.foreach(c => c.put(k, v)))
    configs
  }

  override def extraControllerSecurityProtocols(): Seq[SecurityProtocol] = {
    Seq(extraControllerSecurityProtocol)
  }

  @AfterEach
  override def tearDown(): Unit = {
    // Ensure semaphore doesn't block shutdown even if test has failed
    val semaphore = SslAdminIntegrationTest.semaphore
    SslAdminIntegrationTest.semaphore = None
    semaphore.foreach(s => s.release(s.getQueueLength))

    super.tearDown()
  }

  @ParameterizedTest
  @ValueSource(strings = Array("kraft"))
  def testListNodesFromControllersIncludingFencedBrokers(quorum: String): Unit = {
    useBoostrapControllers()
    client = createAdminClient
    val result = client.describeCluster(new DescribeClusterOptions().includeFencedBrokers(true))
    val exception = assertThrows(classOf[Exception], () => { result.nodes().get()})
    assertTrue(exception.getCause.getCause.getMessage.contains("Cannot request fenced brokers from controller endpoint"))
  }

  @ParameterizedTest
  @ValueSource(strings = Array("kraft"))
  def testListNodesFromControllers(quorum: String): Unit = {
    useBoostrapControllers()
    client = createAdminClient
    val result = client.describeCluster(new DescribeClusterOptions())
    assertTrue(result.nodes().get().size().equals(controllerServers.size))
  }

  @ParameterizedTest
  @ValueSource(strings = Array("kraft"))
  def testAclUpdatesUsingSynchronousAuthorizer(quorum: String): Unit = {
    verifyAclUpdates()
  }

  @ParameterizedTest
  @ValueSource(strings = Array("kraft"))
  def testAclUpdatesUsingAsynchronousAuthorizer(quorum: String): Unit = {
    SslAdminIntegrationTest.executor = Some(Executors.newSingleThreadExecutor)
    verifyAclUpdates()
  }

  /**
   * Verify that ACL updates using synchronous authorizer are performed synchronously
   * on request threads without any performance overhead introduced by a purgatory.
   */
  @ParameterizedTest
  @ValueSource(strings = Array("kraft"))
  def testSynchronousAuthorizerAclUpdatesBlockRequestThreads(quorum: String): Unit = {
    val testSemaphore = new Semaphore(0)
    SslAdminIntegrationTest.semaphore = Some(testSemaphore)
    waitForNoBlockedRequestThreads()

    useBoostrapControllers()
    // Queue requests until all threads are blocked. ACL create requests are sent to least loaded
    // node, so we may need more than `numRequestThreads` requests to block all threads.
    val aclFutures = mutable.Buffer[CreateAclsResult]()
    // In KRaft mode, ACL creation is handled exclusively by controller servers, not brokers.
    // Therefore, only the number of controller I/O threads is relevant in this context.
    val numReqThreads = controllerServers.head.config.numIoThreads * controllerServers.size
    while (blockedRequestThreads.size < numReqThreads) {
      aclFutures += createAdminClient.createAcls(List(acl2).asJava)
      assertTrue(aclFutures.size < numReqThreads * 10,
        s"Request threads not blocked numRequestThreads=$numReqThreads blocked=$blockedRequestThreads aclFutures=${aclFutures.size}")
    }
    assertEquals(0, purgatoryMetric("NumDelayedOperations"))
    assertEquals(0, purgatoryMetric("PurgatorySize"))

    // Verify that operations on other clients are blocked
    val listPartitionReassignmentsFuture = createAdminClient.listPartitionReassignments().reassignments()
    assertFalse(listPartitionReassignmentsFuture.isDone)

    // Release the semaphore and verify that all requests complete
    testSemaphore.release(aclFutures.size)
    waitForNoBlockedRequestThreads()
    assertNotNull(listPartitionReassignmentsFuture.get(10, TimeUnit.SECONDS))
    // If any of the requests time out since we were blocking the threads earlier, retry the request.
    val numTimedOut = aclFutures.count { future =>
      try {
        future.all().get()
        false
      } catch {
        case e: ExecutionException =>
          if (e.getCause.isInstanceOf[org.apache.kafka.common.errors.TimeoutException])
            true
          else
            throw e.getCause
      }
    }
    (0 until numTimedOut)
      .map(_ => createAdminClient.createAcls(List(acl2).asJava))
      .foreach(_.all().get(30, TimeUnit.SECONDS))
  }

  /**
   * Verify that ACL updates using an asynchronous authorizer are completed asynchronously
   * using a purgatory, enabling other requests to be processed even when ACL updates are blocked.
   */
  @ParameterizedTest
  @ValueSource(strings = Array("kraft"))
  def testAsynchronousAuthorizerAclUpdatesDontBlockRequestThreads(quorum: String): Unit = {
    SslAdminIntegrationTest.executor = Some(Executors.newSingleThreadExecutor)
    val testSemaphore = new Semaphore(0)
    SslAdminIntegrationTest.semaphore = Some(testSemaphore)

    waitForNoBlockedRequestThreads()

    useBoostrapControllers()
    // In KRaft mode, ACL creation is handled exclusively by controller servers, not brokers.
    // Therefore, only the number of controller I/O threads is relevant in this context.
    val numReqThreads = controllerServers.head.config.numIoThreads * controllerServers.size
    val aclFutures = (0 until numReqThreads).map(_ => createAdminClient.createAcls(List(acl2).asJava))

    waitForNoBlockedRequestThreads()
    assertTrue(aclFutures.forall(future => !future.all.isDone))
    // Other requests should succeed even though ACL updates are blocked
    assertNotNull(createAdminClient.listPartitionReassignments().reassignments().get(10, TimeUnit.SECONDS))
    TestUtils.waitUntilTrue(() => purgatoryMetric("PurgatorySize") > 0, "PurgatorySize metrics not updated")
    TestUtils.waitUntilTrue(() => purgatoryMetric("NumDelayedOperations") > 0, "NumDelayedOperations metrics not updated")

    // Release the semaphore and verify that ACL update requests complete
    testSemaphore.release(aclFutures.size)
    aclFutures.foreach(_.all.get())
    assertEquals(0, purgatoryMetric("NumDelayedOperations"))
  }

  private def verifyAclUpdates(): Unit = {
    val acl = new AclBinding(new ResourcePattern(ResourceType.TOPIC, "mytopic3", PatternType.LITERAL),
      new AccessControlEntry("User:ANONYMOUS", "*", AclOperation.DESCRIBE, AclPermissionType.ALLOW))
    val clientPrincipal = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, SslAdminIntegrationTest.clientCn)

    def validateRequestContext(context: AuthorizableRequestContext, apiKey: ApiKeys): Unit = {
      assertEquals(SecurityProtocol.SSL, context.securityProtocol)
      assertEquals("SSL", context.listenerName)
      assertEquals(clientPrincipal, context.principal)
      assertEquals(apiKey.id.toInt, context.requestType)
      assertEquals(apiKey.latestVersion.toInt, context.requestVersion)
      assertTrue(context.correlationId > 0, s"Invalid correlation id: ${context.correlationId}")
      assertTrue(context.clientId.startsWith("adminclient"), s"Invalid client id: ${context.clientId}")
      assertTrue(context.clientAddress.isLoopbackAddress, s"Invalid host address: ${context.clientAddress}")
    }

    val testSemaphore = new Semaphore(0)
    SslAdminIntegrationTest.semaphore = Some(testSemaphore)

    useBoostrapControllers()
    client = createAdminClient
    val results = client.createAcls(List(acl2, acl3).asJava).values
    assertEquals(Set(acl2, acl3), results.keySet().asScala)
    assertFalse(results.values.asScala.exists(_.isDone))
    TestUtils.waitUntilTrue(() => testSemaphore.hasQueuedThreads, "Authorizer not blocked in createAcls")
    testSemaphore.release()
    results.values.forEach(_.get)
    validateRequestContext(SslAdminIntegrationTest.lastUpdateRequestContext.get, ApiKeys.CREATE_ACLS)

    testSemaphore.acquire()
    val results2 = client.deleteAcls(List(acl.toFilter, acl2.toFilter, acl3.toFilter).asJava).values
    assertEquals(Set(acl.toFilter, acl2.toFilter, acl3.toFilter), results2.keySet.asScala)
    assertFalse(results2.values.asScala.exists(_.isDone))
    TestUtils.waitUntilTrue(() => testSemaphore.hasQueuedThreads, "Authorizer not blocked in deleteAcls")
    testSemaphore.release()
    results.values.forEach(_.get)
    assertEquals(0, results2.get(acl.toFilter).get.values.size())
    assertEquals(Set(acl2), results2.get(acl2.toFilter).get.values.asScala.map(_.binding).toSet)
    assertEquals(Set(acl3), results2.get(acl3.toFilter).get.values.asScala.map(_.binding).toSet)
    validateRequestContext(SslAdminIntegrationTest.lastUpdateRequestContext.get, ApiKeys.DELETE_ACLS)
  }

  private def blockedRequestThreads: List[Thread] = {
    val requestThreads = Thread.getAllStackTraces.keySet.asScala
      .filter(_.getName.contains("data-plane-kafka-request-handler"))
    assertEquals(numRequestThreads, requestThreads.size)
    requestThreads.filter(_.getState == Thread.State.WAITING).toList
  }

  private def numRequestThreads = {
    brokers.head.config.numIoThreads * (brokers.size + controllerServers.size)
  }

  private def waitForNoBlockedRequestThreads(): Unit = {
    val (blockedThreads, _) = TestUtils.computeUntilTrue(blockedRequestThreads)(_.isEmpty)
    assertEquals(List.empty, blockedThreads)
  }

  private def purgatoryMetric(name: String): Int = {
    val allMetrics = KafkaYammerMetrics.defaultRegistry.allMetrics.asScala
    val metrics = allMetrics.filter { case (metricName, _) =>
      metricName.getMBeanName.contains("delayedOperation=AlterAcls") && metricName.getMBeanName.contains(s"name=$name")
    }.values.toList
    assertTrue(metrics.nonEmpty, s"Unable to find metric $name: allMetrics: ${allMetrics.keySet.map(_.getMBeanName)}")
    metrics.map(_.asInstanceOf[Gauge[Int]].value).sum
  }

  // Override the CN to create a principal based on it
  override def superuserSecurityProps(certAlias: String): Properties = {
    val props = JaasTestUtils.securityConfigs(ConnectionMode.CLIENT, securityProtocol, OptionConverters.toJava(trustStoreFile),
      certAlias, SslAdminIntegrationTest.superuserCn, OptionConverters.toJava(clientSaslProperties))
    props.remove(SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG)
    props
  }

  // Override the CN to create a principal based on it
  override def clientSecurityProps(certAlias: String): Properties = {
    val props = JaasTestUtils.securityConfigs(ConnectionMode.CLIENT, securityProtocol, OptionConverters.toJava(trustStoreFile),
      certAlias, SslAdminIntegrationTest.clientCn, OptionConverters.toJava(clientSaslProperties))
    props.remove(SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG)
    props
  }

  private def useBoostrapControllers(): Unit = {
    val controllerListenerName = ListenerName.forSecurityProtocol(extraControllerSecurityProtocol)
    val config = controllerServers.map { s =>
      val listener = s.config.effectiveAdvertisedControllerListeners
        .find(_.listenerName == controllerListenerName)
        .getOrElse(throw new IllegalArgumentException(s"Could not find listener with name $controllerListenerName"))
      Utils.formatAddress(listener.host, s.socketServer.boundPort(controllerListenerName))
    }.mkString(",")

    adminClientConfig.remove(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG)
    adminClientConfig.put(AdminClientConfig.BOOTSTRAP_CONTROLLERS_CONFIG, config)
  }
}
