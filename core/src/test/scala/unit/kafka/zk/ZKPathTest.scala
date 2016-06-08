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

  val path = "/some_dir"
  val zkSessionTimeoutMs = 1000
  def zkConnectWithInvalidRoot: String = zkConnect + "/ghost"

  @Test
  def testCreatePersistentPathThrowsException {
    val config = new ConsumerConfig(TestUtils.createConsumerProperties(zkConnectWithInvalidRoot,
      "test", "1"))
    val zkUtils = ZkUtils(zkConnectWithInvalidRoot, zkSessionTimeoutMs,
      config.zkConnectionTimeoutMs, false)
    try {
      ZkPath.resetNamespaceCheckedState
      zkUtils.createPersistentPath(path)
      fail("Failed to throw ConfigException for missing zookeeper root node")
    } catch {
      case configException: ConfigException =>
      case exception: Throwable => fail("Should have thrown ConfigException")
    }
    zkUtils.close()
  }

  @Test
  def testCreatePersistentPath {
    val config = new ConsumerConfig(TestUtils.createConsumerProperties(zkConnect, "test", "1"))
    val zkUtils = ZkUtils(zkConnect, zkSessionTimeoutMs, config.zkConnectionTimeoutMs, false)
    try {
      ZkPath.resetNamespaceCheckedState
      zkUtils.createPersistentPath(path)
    } catch {
      case exception: Throwable => fail("Failed to create persistent path")
    }

    assertTrue("Failed to create persistent path", zkUtils.pathExists(path))
    zkUtils.close()
  }

  @Test
  def testMakeSurePersistsPathExistsThrowsException {
    val config = new ConsumerConfig(TestUtils.createConsumerProperties(zkConnectWithInvalidRoot, "test", "1"))
    val zkUtils = ZkUtils(zkConnectWithInvalidRoot, zkSessionTimeoutMs, config.zkConnectionTimeoutMs, false)
    try {
      ZkPath.resetNamespaceCheckedState
      zkUtils.makeSurePersistentPathExists(path)
      fail("Failed to throw ConfigException for missing zookeeper root node")
    } catch {
      case configException: ConfigException =>
      case exception: Throwable => fail("Should have thrown ConfigException")
    }
    zkUtils.close()
  }

  @Test
  def testMakeSurePersistsPathExists {
    val config = new ConsumerConfig(TestUtils.createConsumerProperties(zkConnect, "test", "1"))
    val zkUtils = ZkUtils(zkConnect, zkSessionTimeoutMs, config.zkConnectionTimeoutMs, false)
    try {
      ZkPath.resetNamespaceCheckedState
      zkUtils.makeSurePersistentPathExists(path)
    } catch {
      case exception: Throwable => fail("Failed to create persistent path")
    }

    assertTrue("Failed to create persistent path", zkUtils.pathExists(path))
    zkUtils.close()
  }

  @Test
  def testCreateEphemeralPathThrowsException {
    val config = new ConsumerConfig(TestUtils.createConsumerProperties(zkConnectWithInvalidRoot, "test", "1"))
    val zkUtils = ZkUtils(zkConnectWithInvalidRoot, zkSessionTimeoutMs, config.zkConnectionTimeoutMs, false)
    try {
      ZkPath.resetNamespaceCheckedState
      zkUtils.createEphemeralPathExpectConflict(path, "somedata")
      fail("Failed to throw ConfigException for missing zookeeper root node")
    } catch {
      case configException: ConfigException =>
      case exception: Throwable => fail("Should have thrown ConfigException")
    }
    zkUtils.close()
  }

  @Test
  def testCreateEphemeralPathExists {
    val config = new ConsumerConfig(TestUtils.createConsumerProperties(zkConnect, "test", "1"))
    val zkUtils = ZkUtils(zkConnect, zkSessionTimeoutMs, config.zkConnectionTimeoutMs, false)
    try {
      ZkPath.resetNamespaceCheckedState
      zkUtils.createEphemeralPathExpectConflict(path, "somedata")
    } catch {
      case exception: Throwable => fail("Failed to create ephemeral path")
    }

    assertTrue("Failed to create ephemeral path", zkUtils.pathExists(path))
    zkUtils.close()
  }

  @Test
  def testCreatePersistentSequentialThrowsException {
    val config = new ConsumerConfig(TestUtils.createConsumerProperties(zkConnectWithInvalidRoot,
      "test", "1"))
    val zkUtils = ZkUtils(zkConnectWithInvalidRoot, zkSessionTimeoutMs, config.zkConnectionTimeoutMs, false)
    try {
      ZkPath.resetNamespaceCheckedState
      zkUtils.createSequentialPersistentPath(path)
      fail("Failed to throw ConfigException for missing zookeeper root node")
    } catch {
      case configException: ConfigException =>
      case exception: Throwable => fail("Should have thrown ConfigException")
    }
    zkUtils.close()
  }

  @Test
  def testCreatePersistentSequentialExists {
    val config = new ConsumerConfig(TestUtils.createConsumerProperties(zkConnect, "test", "1"))
    val zkUtils = ZkUtils(zkConnect, zkSessionTimeoutMs, config.zkConnectionTimeoutMs, false)

    var actualPath: String = ""
    try {
      ZkPath.resetNamespaceCheckedState
      actualPath = zkUtils.createSequentialPersistentPath(path)
    } catch {
      case exception: Throwable => fail("Failed to create persistent path")
    }

    assertTrue("Failed to create persistent path", zkUtils.pathExists(actualPath))
    zkUtils.close()
  }
}
