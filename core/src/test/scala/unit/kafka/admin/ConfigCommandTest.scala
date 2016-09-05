/**
  * Licensed to the Apache Software Foundation (ASF) under one or more
  * contributor license agreements.  See the NOTICE file distributed with
  * this work for additional information regarding copyright ownership.
  * The ASF licenses this file to You under the Apache License, Version 2.0
  * (the "License"); you may not use this file except in compliance with
  * the License.  You may obtain a copy of the License at
  *
  * http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software
  * distributed under the License is distributed on an "AS IS" BASIS,
  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  * See the License for the specific language governing permissions and
  * limitations under the License.
  */
package kafka.admin

import java.util.Properties

import kafka.admin.ConfigCommand.ConfigCommandOptions
import org.junit.Assert._
import org.junit.Test
import kafka.utils.{ZkUtils, Logging}
import kafka.zk.ZooKeeperTestHarness
import unit.kafka.admin.TestAdminUtils

class ConfigCommandTest extends ZooKeeperTestHarness with Logging {
  @Test
  def shouldParseArgumentsForClientsEntityType() {
    testArgumentParse("clients")
  }

  @Test
  def shouldParseArgumentsForTopicsEntityType() {
    testArgumentParse("topics")
  }

  @Test
  def shouldParseArgumentsForBrokersEntityType() {
    testArgumentParse("brokers")
  }

  def testArgumentParse(entityType: String) = {
    // Should parse correctly
    var createOpts = new ConfigCommandOptions(Array("--zookeeper", zkConnect,
      "--entity-name", "x",
      "--entity-type", entityType,
      "--describe"))
    createOpts.checkArgs()

    // For --alter and added config
    createOpts = new ConfigCommandOptions(Array("--zookeeper", zkConnect,
      "--entity-name", "x",
      "--entity-type", entityType,
      "--alter",
      "--add-config", "a=b,c=d"))
    createOpts.checkArgs()

    // For alter and deleted config
    createOpts = new ConfigCommandOptions(Array("--zookeeper", zkConnect,
      "--entity-name", "x",
      "--entity-type", entityType,
      "--alter",
      "--delete-config", "a,b,c"))
    createOpts.checkArgs()

    // For alter and both added, deleted config
    createOpts = new ConfigCommandOptions(Array("--zookeeper", zkConnect,
      "--entity-name", "x",
      "--entity-type", entityType,
      "--alter",
      "--add-config", "a=b,c=d",
      "--delete-config", "a"))
    createOpts.checkArgs()

    val addedProps = ConfigCommand.parseConfigsToBeAdded(createOpts)
    assertEquals(2, addedProps.size())
    assertEquals("b", addedProps.getProperty("a"))
    assertEquals("d", addedProps.getProperty("c"))

    val deletedProps = ConfigCommand.parseConfigsToBeDeleted(createOpts)
    assertEquals(1, deletedProps.size)
    assertEquals("a", deletedProps.head)
  }

  @Test(expected = classOf[IllegalArgumentException])
  def shouldFailIfUnrecognisedEntityType(): Unit = {
    val createOpts = new ConfigCommandOptions(Array("--zookeeper", zkConnect,
      "--entity-name", "client", "--entity-type", "not-recognised", "--alter", "--add-config", "a=b,c=d"))
    ConfigCommand.alterConfig(null, createOpts, new TestAdminUtils)
  }

  @Test
  def shouldAddClientConfig(): Unit = {
    val createOpts = new ConfigCommandOptions(Array("--zookeeper", zkConnect,
      "--entity-name", "my-client-id",
      "--entity-type", "clients",
      "--alter",
      "--add-config", "a=b,c=d"))

    val configChange = new TestAdminUtils {
      override def changeClientIdConfig(zkUtils: ZkUtils, clientId: String, configChange: Properties): Unit = {
        assertEquals("my-client-id", clientId)
        assertEquals("b", configChange.get("a"))
        assertEquals("d", configChange.get("c"))
      }
    }
    ConfigCommand.alterConfig(null, createOpts, configChange)
  }

  @Test
  def shouldAddTopicConfig(): Unit = {
    val createOpts = new ConfigCommandOptions(Array("--zookeeper", zkConnect,
      "--entity-name", "my-topic",
      "--entity-type", "topics",
      "--alter",
      "--add-config", "a=b,c=d"))

    val configChange = new TestAdminUtils {
      override def changeTopicConfig(zkUtils: ZkUtils, topic: String, configChange: Properties): Unit = {
        assertEquals("my-topic", topic)
        assertEquals("b", configChange.get("a"))
        assertEquals("d", configChange.get("c"))
      }
    }
    ConfigCommand.alterConfig(null, createOpts, configChange)
  }

  @Test
  def shouldAddBrokerConfig(): Unit = {
    val createOpts = new ConfigCommandOptions(Array("--zookeeper", zkConnect,
      "--entity-name", "1",
      "--entity-type", "brokers",
      "--alter",
      "--add-config", "a=b,c=d"))

    val configChange = new TestAdminUtils {
      override def changeBrokerConfig(zkUtils: ZkUtils, brokerIds: Seq[Int], configChange: Properties): Unit = {
        assertEquals(Seq(1), brokerIds)
        assertEquals("b", configChange.get("a"))
        assertEquals("d", configChange.get("c"))
      }
    }
    ConfigCommand.alterConfig(null, createOpts, configChange)
  }

  @Test (expected = classOf[IllegalArgumentException])
  def shouldNotUpdateBrokerConfigIfMalformed(): Unit = {
    val createOpts = new ConfigCommandOptions(Array("--zookeeper", zkConnect,
      "--entity-name", "1,2,3", //Don't support multiple brokers currently
      "--entity-type", "brokers",
      "--alter",
      "--add-config", "a=b"))

    val configChange = new TestAdminUtils {
      override def changeBrokerConfig(zkUtils: ZkUtils, brokerIds: Seq[Int], configChange: Properties): Unit = {
        assertEquals(Seq(1), brokerIds)
      }
    }
    ConfigCommand.alterConfig(null, createOpts, configChange)
  }

  @Test
  def shouldDeleteBrokerConfig(): Unit = {
    val createOpts = new ConfigCommandOptions(Array("--zookeeper", zkConnect,
      "--entity-name", "1",
      "--entity-type", "brokers",
      "--alter",
      "--delete-config", "a,c"))

    val configChange = new TestAdminUtils {
      override def fetchEntityConfig(zkUtils: ZkUtils, entityType: String, entityName: String): Properties = {
        val properties: Properties = new Properties
        properties.put("a", "b")
        properties.put("c", "d")
        properties.put("e", "f")
        properties
      }

      override def changeBrokerConfig(zkUtils: ZkUtils, brokerIds: Seq[Int], configChange: Properties): Unit = {
        assertEquals("f", configChange.get("e"))
        assertEquals(1, configChange.size())
      }
    }
    ConfigCommand.alterConfig(null, createOpts, configChange)
  }
}
