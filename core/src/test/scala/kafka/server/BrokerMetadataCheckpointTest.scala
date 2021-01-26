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
package kafka.server

import java.io.File
import java.util.Properties

import kafka.server.KafkaRaftServer.{BrokerRole, ControllerRole}
import org.apache.kafka.common.Uuid
import org.apache.kafka.common.utils.Utils
import org.apache.kafka.test.TestUtils
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test

class BrokerMetadataCheckpointTest {

  @Test
  def testReadWithNonExistentFile(): Unit = {
    assertEquals(None, new BrokerMetadataCheckpoint(new File("path/that/does/not/exist")).read())
  }

  @Test
  def testCreateZkMetadataProperties(): Unit = {
    val meta = ZkMetaProperties("7bc79ca1-9746-42a3-a35a-efb3cde44492", 3)
    val properties = meta.toProperties
    val parsed = RawMetaProperties(properties)
    assertEquals(0, parsed.version)
    assertEquals(Some(meta.clusterId), parsed.clusterId)
    assertEquals(Some(meta.brokerId), parsed.brokerId)
  }

  @Test
  def testParseRawMetaPropertiesWithoutVersion(): Unit = {
    val brokerId = 1
    val clusterId = "7bc79ca1-9746-42a3-a35a-efb3cde44492"

    val properties = new Properties()
    properties.put(RawMetaProperties.BrokerIdKey, brokerId.toString)
    properties.put(RawMetaProperties.ClusterIdKey, clusterId)

    val parsed = RawMetaProperties(properties)
    assertEquals(Some(brokerId), parsed.brokerId)
    assertEquals(Some(clusterId), parsed.clusterId)
    assertEquals(0, parsed.version)
  }

  @Test
  def testRawPropertiesWithInvalidBrokerId(): Unit = {
    val properties = new Properties()
    properties.put(RawMetaProperties.BrokerIdKey, "oof")
    val parsed = RawMetaProperties(properties)
    assertThrows(classOf[RuntimeException], () => parsed.brokerId)
  }

  @Test
  def testCreateMetadataProperties(): Unit = {
    val meta = MetaProperties(
      clusterId = Uuid.fromString("H3KKO4NTRPaCWtEmm3vW7A"),
      brokerId = Some(5),
      controllerId = None
    )
    val properties = RawMetaProperties(meta.toProperties)
    val meta2 = MetaProperties.parse(properties, Set(BrokerRole))
    assertEquals(meta, meta2)
  }

  @Test
  def testMetaPropertiesWithMissingVersion(): Unit = {
    val properties = RawMetaProperties()
    properties.clusterId = "H3KKO4NTRPaCWtEmm3vW7A"
    properties.brokerId = 1
    assertThrows(classOf[RuntimeException], () => MetaProperties.parse(properties, Set(BrokerRole)))
  }

  @Test
  def testMetaPropertiesWithInvalidClusterId(): Unit = {
    val properties = RawMetaProperties()
    properties.version = 1
    properties.clusterId = "not a valid uuid"
    properties.brokerId = 1
    assertThrows(classOf[RuntimeException], () => MetaProperties.parse(properties, Set(BrokerRole)))
  }

  @Test
  def testMetaPropertiesWithMissingBrokerId(): Unit = {
    val properties = RawMetaProperties()
    properties.version = 1
    properties.clusterId = "H3KKO4NTRPaCWtEmm3vW7A"
    assertThrows(classOf[RuntimeException], () => MetaProperties.parse(properties, Set(BrokerRole)))
  }

  @Test
  def testMetaPropertiesWithMissingControllerId(): Unit = {
    val properties = RawMetaProperties()
    properties.version = 1
    properties.clusterId = "H3KKO4NTRPaCWtEmm3vW7A"
    assertThrows(classOf[RuntimeException], () => MetaProperties.parse(properties, Set(ControllerRole)))
  }

  @Test
  def testGetBrokerMetadataAndOfflineDirsWithNonexistentDirectories(): Unit = {
    val tempDir = TestUtils.tempDirectory()
    try {
      assertEquals((new RawMetaProperties(), Seq(tempDir.getAbsolutePath)),
        BrokerMetadataCheckpoint.getBrokerMetadataAndOfflineDirs(
          Seq(tempDir.getAbsolutePath), false))
      assertEquals((new RawMetaProperties(), Seq()),
        BrokerMetadataCheckpoint.getBrokerMetadataAndOfflineDirs(
          Seq(tempDir.getAbsolutePath), true))
    } finally {
      Utils.delete(tempDir)
    }
  }
}
