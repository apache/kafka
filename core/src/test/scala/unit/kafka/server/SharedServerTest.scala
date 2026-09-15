/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.server

import java.io.File
import java.net.InetSocketAddress
import java.util
import java.util.concurrent.{CompletableFuture, CountDownLatch, TimeUnit, TimeoutException}

import kafka.utils.TestUtils
import org.apache.kafka.common.DirectoryId
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.utils.{Time, Utils => KafkaUtils}
import org.apache.kafka.metadata.properties.{MetaProperties, MetaPropertiesEnsemble, MetaPropertiesVersion, PropertiesUtils}
import org.apache.kafka.network.SocketServerConfigs
import org.apache.kafka.raft.{KRaftConfigs, QuorumConfig}
import org.apache.kafka.server.ServerSocketFactory
import org.apache.kafka.server.config.ServerLogConfigs
import org.apache.kafka.server.fault.FaultHandler
import org.junit.jupiter.api.Assertions.{assertFalse, assertNotNull, assertTrue}
import org.junit.jupiter.api.Test

class SharedServerTest {
  private val clusterId = "H3KKO4NTRPaCWtEmm3vW7A"

  @Test
  def testMetadataLoaderFaultHandlerDoesNotWaitForSharedServerLock(): Unit = {
    val logDir = TestUtils.tempDir()
    val faultHandlerFactory = new CapturingFaultHandlerFactory
    val server = newSharedServer(logDir, faultHandlerFactory)
    try {
      val faultHandler = server.metadataLoaderFaultHandler
      assertNotNull(faultHandler)
      assertNotNull(faultHandlerFactory.action)

      val lockAcquired = new CountDownLatch(1)
      val releaseLock = new CountDownLatch(1)
      val lockHolder = new Thread(() => server.synchronized {
        lockAcquired.countDown()
        releaseLock.await()
      })
      lockHolder.start()
      assertTrue(lockAcquired.await(5, TimeUnit.SECONDS))

      val actionComplete = CompletableFuture.runAsync(() => faultHandlerFactory.action.run())
      var actionTimedOut = false
      try {
        actionComplete.get(5, TimeUnit.SECONDS)
      } catch {
        case _: TimeoutException => actionTimedOut = true
      } finally {
        releaseLock.countDown()
        lockHolder.join(5000)
        actionComplete.get(5, TimeUnit.SECONDS)
      }

      assertFalse(actionTimedOut, "The metadata fault action must not wait for the SharedServer monitor")
    } finally {
      KafkaUtils.delete(logDir)
    }
  }

  private def newSharedServer(logDir: File, faultHandlerFactory: FaultHandlerFactory): SharedServer = {
    val metaProperties = new MetaProperties.Builder().
      setVersion(MetaPropertiesVersion.V1).
      setClusterId(clusterId).
      setNodeId(0).
      setDirectoryId(DirectoryId.random()).
      build()
    PropertiesUtils.writePropertiesFile(
      metaProperties.toProperties,
      new File(logDir, MetaPropertiesEnsemble.META_PROPERTIES_NAME).getAbsolutePath,
      false)

    val metaPropertiesEnsemble = new MetaPropertiesEnsemble.Loader().
      addLogDirs(util.List.of(logDir.getAbsolutePath)).
      addMetadataLogDir(logDir.getAbsolutePath).
      load()

    val properties = TestUtils.createBrokerConfig(0)
    properties.put(KRaftConfigs.PROCESS_ROLES_CONFIG, "controller")
    properties.put(ServerLogConfigs.LOG_DIR_CONFIG, logDir.getAbsolutePath)
    properties.put(SocketServerConfigs.LISTENERS_CONFIG, "CONTROLLER://localhost:0")
    properties.put(SocketServerConfigs.ADVERTISED_LISTENERS_CONFIG, "CONTROLLER://localhost:0")
    properties.put(SocketServerConfigs.LISTENER_SECURITY_PROTOCOL_MAP_CONFIG, "CONTROLLER:PLAINTEXT")
    properties.put(KRaftConfigs.CONTROLLER_LISTENER_NAMES_CONFIG, "CONTROLLER")
    properties.put(QuorumConfig.QUORUM_VOTERS_CONFIG, "0@localhost:0")
    val config = KafkaConfig.fromProps(properties, doLog = false)

    new SharedServer(
      config,
      metaPropertiesEnsemble,
      Time.SYSTEM,
      new Metrics(),
      new CompletableFuture[util.Map[Integer, InetSocketAddress]](),
      util.Collections.emptyList[InetSocketAddress](),
      faultHandlerFactory,
      ServerSocketFactory.INSTANCE)
  }

  private class CapturingFaultHandlerFactory extends FaultHandlerFactory {
    var action: Runnable = _

    override def build(name: String, fatal: Boolean, action: Runnable): FaultHandler = {
      if (name == "metadata loading") {
        this.action = action
      }
      new FaultHandler {
        override def handleFault(failureMessage: String, cause: Throwable): RuntimeException = null
      }
    }
  }
}
