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

package unit.kafka.zk

import junit.framework.Assert
import kafka.consumer.ConsumerConfig
import kafka.utils.{TestUtils, ZKStringSerializer, ZkUtils}
import kafka.zk.ZooKeeperTestHarness
import org.I0Itec.zkclient.ZkClient
import org.apache.kafka.common.config.ConfigException
import org.scalatest.junit.JUnit3Suite

class ZKPathTest extends JUnit3Suite with ZooKeeperTestHarness {

  val path: String = "/some_dir"
  val zkSessionTimeoutMs = 1000
  val zkConnectWithInvalidRoot: String = zkConnect + "/ghost"

  def testCreatePersistentPathThrowsException {
    val config = new ConsumerConfig(TestUtils.createConsumerProperties(zkConnectWithInvalidRoot,
      "test", "1"))
    var zkClient = new ZkClient(zkConnectWithInvalidRoot, zkSessionTimeoutMs,
      config.zkConnectionTimeoutMs,
      ZKStringSerializer)
    try {
      ZkUtils.createPersistentPath(zkClient, path)
      fail("Failed to throw ConfigException for missing zookeeper root node")
    } catch {
      case configException: ConfigException =>
      case exception: Throwable => fail("Should have thrown ConfigException")
    }
  }

  def testCreatePersistentPath {
    val config = new ConsumerConfig(TestUtils.createConsumerProperties(zkConnect, "test", "1"))
    var zkClient = new ZkClient(zkConnect, zkSessionTimeoutMs, config.zkConnectionTimeoutMs,
      ZKStringSerializer)
    try {
      ZkUtils.createPersistentPath(zkClient, path)
    } catch {
      case exception: Throwable => fail("Failed to create persistent path")
    }

    Assert.assertTrue("Failed to create persistent path", ZkUtils.pathExists(zkClient, path));
  }

  def testMakeSurePersistsPathExistsThrowsException {
    val config = new ConsumerConfig(TestUtils.createConsumerProperties(zkConnectWithInvalidRoot,
      "test", "1"))
    var zkClient = new ZkClient(zkConnectWithInvalidRoot, zkSessionTimeoutMs,
      config.zkConnectionTimeoutMs,
      ZKStringSerializer)
    try {
      ZkUtils.makeSurePersistentPathExists(zkClient, path)
      fail("Failed to throw ConfigException for missing zookeeper root node")
    } catch {
      case configException: ConfigException =>
      case exception: Throwable => fail("Should have thrown ConfigException")
    }
  }

  def testMakeSurePersistsPathExists {
    val config = new ConsumerConfig(TestUtils.createConsumerProperties(zkConnect, "test", "1"))
    var zkClient = new ZkClient(zkConnect, zkSessionTimeoutMs, config.zkConnectionTimeoutMs,
      ZKStringSerializer)
    try {
      ZkUtils.makeSurePersistentPathExists(zkClient, path)
    } catch {
      case exception: Throwable => fail("Failed to create persistent path")
    }

    Assert.assertTrue("Failed to create persistent path", ZkUtils.pathExists(zkClient, path));
  }

  def testCreateEphemeralPathThrowsException {
    val config = new ConsumerConfig(TestUtils.createConsumerProperties(zkConnectWithInvalidRoot,
      "test", "1"))
    var zkClient = new ZkClient(zkConnectWithInvalidRoot, zkSessionTimeoutMs,
      config.zkConnectionTimeoutMs,
      ZKStringSerializer)
    try {
      ZkUtils.createEphemeralPathExpectConflict(zkClient, path, "somedata")
      fail("Failed to throw ConfigException for missing zookeeper root node")
    } catch {
      case configException: ConfigException =>
      case exception: Throwable => fail("Should have thrown ConfigException")
    }
  }

  def testCreateEphemeralPathExists {
    val config = new ConsumerConfig(TestUtils.createConsumerProperties(zkConnect, "test", "1"))
    var zkClient = new ZkClient(zkConnect, zkSessionTimeoutMs, config.zkConnectionTimeoutMs,
      ZKStringSerializer)
    try {
      ZkUtils.createEphemeralPathExpectConflict(zkClient, path, "somedata")
    } catch {
      case exception: Throwable => fail("Failed to create ephemeral path")
    }

    Assert.assertTrue("Failed to create ephemeral path", ZkUtils.pathExists(zkClient, path));
  }

  def testCreatePersistentSequentialThrowsException {
    val config = new ConsumerConfig(TestUtils.createConsumerProperties(zkConnectWithInvalidRoot,
      "test", "1"))
    var zkClient = new ZkClient(zkConnectWithInvalidRoot, zkSessionTimeoutMs,
      config.zkConnectionTimeoutMs,
      ZKStringSerializer)
    try {
      ZkUtils.createSequentialPersistentPath(zkClient, path)
      fail("Failed to throw ConfigException for missing zookeeper root node")
    } catch {
      case configException: ConfigException =>
      case exception: Throwable => fail("Should have thrown ConfigException")
    }
  }

  def testCreatePersistentSequentialExists {
    val config = new ConsumerConfig(TestUtils.createConsumerProperties(zkConnect, "test", "1"))
    var zkClient = new ZkClient(zkConnect, zkSessionTimeoutMs, config.zkConnectionTimeoutMs,
      ZKStringSerializer)

    var actualPath: String = ""
    try {
      actualPath = ZkUtils.createSequentialPersistentPath(zkClient, path)
    } catch {
      case exception: Throwable => fail("Failed to create persistent path")
    }

    Assert.assertTrue("Failed to create persistent path", ZkUtils.pathExists(zkClient, actualPath));
  }
}
