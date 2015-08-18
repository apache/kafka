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

package kafka.zk

import kafka.consumer.ConsumerConfig
import kafka.utils.{ZkPath, TestUtils, ZkUtils}
import org.apache.kafka.common.config.ConfigException
import org.junit.Assert._
import org.junit.Test

class ZKPathTest extends ZooKeeperTestHarness {

  val path: String = "/some_dir"
  val zkSessionTimeoutMs = 1000
  def zkConnectWithInvalidRoot: String = zkConnect + "/ghost"

  @Test
  def testCreatePersistentPathThrowsException {
    val config = new ConsumerConfig(TestUtils.createConsumerProperties(zkConnectWithInvalidRoot,
      "test", "1"))
    var zkClient = ZkUtils.createZkClient(zkConnectWithInvalidRoot, zkSessionTimeoutMs,
      config.zkConnectionTimeoutMs)
    try {
      ZkPath.resetNamespaceCheckedState
      ZkUtils.createPersistentPath(zkClient, path)
      fail("Failed to throw ConfigException for missing zookeeper root node")
    } catch {
      case configException: ConfigException =>
      case exception: Throwable => fail("Should have thrown ConfigException")
    }
  }

  @Test
  def testCreatePersistentPath {
    val config = new ConsumerConfig(TestUtils.createConsumerProperties(zkConnect, "test", "1"))
    var zkClient = ZkUtils.createZkClient(zkConnect, zkSessionTimeoutMs, config.zkConnectionTimeoutMs)
    try {
      ZkPath.resetNamespaceCheckedState
      ZkUtils.createPersistentPath(zkClient, path)
    } catch {
      case exception: Throwable => fail("Failed to create persistent path")
    }

    assertTrue("Failed to create persistent path", ZkUtils.pathExists(zkClient, path))
  }

  @Test
  def testMakeSurePersistsPathExistsThrowsException {
    val config = new ConsumerConfig(TestUtils.createConsumerProperties(zkConnectWithInvalidRoot,
      "test", "1"))
    var zkClient = ZkUtils.createZkClient(zkConnectWithInvalidRoot, zkSessionTimeoutMs,
      config.zkConnectionTimeoutMs)
    try {
      ZkPath.resetNamespaceCheckedState
      ZkUtils.makeSurePersistentPathExists(zkClient, path)
      fail("Failed to throw ConfigException for missing zookeeper root node")
    } catch {
      case configException: ConfigException =>
      case exception: Throwable => fail("Should have thrown ConfigException")
    }
  }

  @Test
  def testMakeSurePersistsPathExists {
    val config = new ConsumerConfig(TestUtils.createConsumerProperties(zkConnect, "test", "1"))
    var zkClient = ZkUtils.createZkClient(zkConnect, zkSessionTimeoutMs, config.zkConnectionTimeoutMs)
    try {
      ZkPath.resetNamespaceCheckedState
      ZkUtils.makeSurePersistentPathExists(zkClient, path)
    } catch {
      case exception: Throwable => fail("Failed to create persistent path")
    }

    assertTrue("Failed to create persistent path", ZkUtils.pathExists(zkClient, path))
  }

  @Test
  def testCreateEphemeralPathThrowsException {
    val config = new ConsumerConfig(TestUtils.createConsumerProperties(zkConnectWithInvalidRoot,
      "test", "1"))
    var zkClient = ZkUtils.createZkClient(zkConnectWithInvalidRoot, zkSessionTimeoutMs,
      config.zkConnectionTimeoutMs)
    try {
      ZkPath.resetNamespaceCheckedState
      ZkUtils.createEphemeralPathExpectConflict(zkClient, path, "somedata")
      fail("Failed to throw ConfigException for missing zookeeper root node")
    } catch {
      case configException: ConfigException =>
      case exception: Throwable => fail("Should have thrown ConfigException")
    }
  }

  @Test
  def testCreateEphemeralPathExists {
    val config = new ConsumerConfig(TestUtils.createConsumerProperties(zkConnect, "test", "1"))
    var zkClient = ZkUtils.createZkClient(zkConnect, zkSessionTimeoutMs, config.zkConnectionTimeoutMs)
    try {
      ZkPath.resetNamespaceCheckedState
      ZkUtils.createEphemeralPathExpectConflict(zkClient, path, "somedata")
    } catch {
      case exception: Throwable => fail("Failed to create ephemeral path")
    }

    assertTrue("Failed to create ephemeral path", ZkUtils.pathExists(zkClient, path))
  }

  @Test
  def testCreatePersistentSequentialThrowsException {
    val config = new ConsumerConfig(TestUtils.createConsumerProperties(zkConnectWithInvalidRoot,
      "test", "1"))
    var zkClient = ZkUtils.createZkClient(zkConnectWithInvalidRoot, zkSessionTimeoutMs,
      config.zkConnectionTimeoutMs)
    try {
      ZkPath.resetNamespaceCheckedState
      ZkUtils.createSequentialPersistentPath(zkClient, path)
      fail("Failed to throw ConfigException for missing zookeeper root node")
    } catch {
      case configException: ConfigException =>
      case exception: Throwable => fail("Should have thrown ConfigException")
    }
  }

  @Test
  def testCreatePersistentSequentialExists {
    val config = new ConsumerConfig(TestUtils.createConsumerProperties(zkConnect, "test", "1"))
    var zkClient = ZkUtils.createZkClient(zkConnect, zkSessionTimeoutMs, config.zkConnectionTimeoutMs)

    var actualPath: String = ""
    try {
      ZkPath.resetNamespaceCheckedState
      actualPath = ZkUtils.createSequentialPersistentPath(zkClient, path)
    } catch {
      case exception: Throwable => fail("Failed to create persistent path")
    }

    assertTrue("Failed to create persistent path", ZkUtils.pathExists(zkClient, actualPath))
  }
}
