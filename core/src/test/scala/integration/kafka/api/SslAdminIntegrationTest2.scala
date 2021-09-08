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

import java.io.File
import java.util
import java.util.concurrent._
import com.yammer.metrics.core.Gauge
import kafka.metrics.KafkaYammerMetrics
import kafka.security.authorizer.AclAuthorizer
import kafka.server.KafkaConfig
import kafka.utils.TestUtils
import org.apache.kafka.clients.admin.{Admin, AdminClientConfig, CreateAclsResult}
import org.apache.kafka.common.acl._
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.apache.kafka.server.authorizer._
import org.junit.jupiter.api.Assertions.{assertEquals, assertFalse, assertNotNull, assertTrue}
import org.junit.jupiter.api.{AfterEach, Test}

import scala.jdk.CollectionConverters._
import scala.collection.mutable

object SslAdminIntegrationTest2 {
  @volatile var semaphore: Option[Semaphore] = None
  @volatile var executor: Option[ExecutorService] = None
  @volatile var lastUpdateRequestContext: Option[AuthorizableRequestContext] = None
  class TestableAclAuthorizer extends AclAuthorizer {
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
              baseFuture.whenComplete { (result, exception) =>
                if (exception != null)
                  resultFuture.completeExceptionally(exception)
                else
                  resultFuture.complete(result)
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
}

class SslAdminIntegrationTest2 extends SaslSslAdminIntegrationTest2 {
  override val authorizationAdmin = new AclAuthorizationAdmin(classOf[SslAdminIntegrationTest2.TestableAclAuthorizer], classOf[AclAuthorizer])

  this.serverConfig.setProperty(KafkaConfig.ZkEnableSecureAclsProp, "true")

  override protected def securityProtocol = SecurityProtocol.SSL
  override protected lazy val trustStoreFile = Some(File.createTempFile("truststore", ".jks"))
  private val adminClients = mutable.Buffer.empty[Admin]

  override def setUpSasl(): Unit = {
    SslAdminIntegrationTest2.semaphore = None
    SslAdminIntegrationTest2.executor = None
    SslAdminIntegrationTest2.lastUpdateRequestContext = None

    startSasl(jaasSections(List.empty, None, ZkSasl))
  }

  @AfterEach
  override def tearDown(): Unit = {
    // Ensure semaphore doesn't block shutdown even if test has failed
    val semaphore = SslAdminIntegrationTest2.semaphore
    SslAdminIntegrationTest2.semaphore = None
    semaphore.foreach(s => s.release(s.getQueueLength))

    adminClients.foreach(_.close())
    super.tearDown()
  }

  /**
   * Verify that ACL updates using synchronous authorizer are performed synchronously
   * on request threads without any performance overhead introduced by a purgatory.
   */
  @Test
  def testSynchronousAuthorizerAclUpdatesBlockRequestThreads(): Unit = {
    val testSemaphore = new Semaphore(0)
    SslAdminIntegrationTest2.semaphore = Some(testSemaphore)
    waitForNoBlockedRequestThreads()

    // Queue requests until all threads are blocked. ACL create requests are sent to least loaded
    // node, so we may need more than `numRequestThreads` requests to block all threads.
    val aclFutures = mutable.Buffer[CreateAclsResult]()
    while (blockedRequestThreads.size < numRequestThreads) {
      aclFutures += createAdminClient.createAcls(List(acl2).asJava)
      assertTrue(aclFutures.size < numRequestThreads * 10,
        s"Request threads not blocked numRequestThreads=$numRequestThreads blocked=$blockedRequestThreads")
    }
    assertEquals(0, purgatoryMetric("NumDelayedOperations"))
    assertEquals(0, purgatoryMetric("PurgatorySize"))

    // Verify that operations on other clients are blocked
    val describeFuture = createAdminClient.describeCluster().clusterId()
    assertFalse(describeFuture.isDone)

    // Release the semaphore and verify that all requests complete
    testSemaphore.release(aclFutures.size)
    waitForNoBlockedRequestThreads()
    assertNotNull(describeFuture.get(10, TimeUnit.SECONDS))
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

  private def createAdminClient: Admin = {
    val config = createConfig
    config.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, "40000")
    val client = Admin.create(config)
    adminClients += client
    client
  }

  private def blockedRequestThreads: List[Thread] = {
    val requestThreads = Thread.getAllStackTraces.keySet.asScala
      .filter(_.getName.contains("data-plane-kafka-request-handler"))
    assertEquals(numRequestThreads, requestThreads.size)
    requestThreads.filter(_.getState == Thread.State.WAITING).toList
  }

  private def numRequestThreads = servers.head.config.numIoThreads * servers.size

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
}
