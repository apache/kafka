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

import kafka.utils.{CoreUtils, Logging}

import java.io.{File, FileOutputStream}
import java.util.Properties
import org.apache.kafka.common.utils.Utils
import org.apache.kafka.test.TestUtils
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test

import scala.collection.mutable

class BrokerMetadataCheckpointTest extends Logging {
  private val clusterIdBase64 = "H3KKO4NTRPaCWtEmm3vW7A"

  @Test
  def testReadWithNonExistentFile(): Unit = {
    assertEquals(None, new BrokerMetadataCheckpoint(new File("path/that/does/not/exist")).read())
  }

  @Test
  def testCreateZkMetadataProperties(): Unit = {
    val meta = ZkMetaProperties("7bc79ca1-9746-42a3-a35a-efb3cde44492", 3)
    val properties = meta.toProperties
    val parsed = new RawMetaProperties(properties)
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

    val parsed = new RawMetaProperties(properties)
    assertEquals(Some(brokerId), parsed.brokerId)
    assertEquals(Some(clusterId), parsed.clusterId)
    assertEquals(0, parsed.version)
  }

  @Test
  def testRawPropertiesWithInvalidBrokerId(): Unit = {
    val properties = new Properties()
    properties.put(RawMetaProperties.BrokerIdKey, "oof")
    val parsed = new RawMetaProperties(properties)
    assertThrows(classOf[RuntimeException], () => parsed.brokerId)
  }

  @Test
  def testCreateMetadataProperties(): Unit = {
    confirmValidForMetaProperties(clusterIdBase64)
  }

  @Test
  def testMetaPropertiesWithMissingVersion(): Unit = {
    val properties = new RawMetaProperties()
    properties.clusterId = clusterIdBase64
    properties.nodeId = 1
    assertThrows(classOf[RuntimeException], () => MetaProperties.parse(properties))
  }

  @Test
  def testMetaPropertiesAllowsHexEncodedUUIDs(): Unit = {
    val clusterId = "7bc79ca1-9746-42a3-a35a-efb3cde44492"
    confirmValidForMetaProperties(clusterId)
  }

  @Test
  def testMetaPropertiesWithNonUuidClusterId(): Unit = {
    val clusterId = "not a valid uuid"
    confirmValidForMetaProperties(clusterId)
  }

  private def confirmValidForMetaProperties(clusterId: String) = {
    val meta = MetaProperties(
      clusterId = clusterId,
      nodeId = 5
    )
    val properties = new RawMetaProperties(meta.toProperties)
    val meta2 = MetaProperties.parse(properties)
    assertEquals(meta, meta2)
  }

  @Test
  def testMetaPropertiesWithMissingBrokerId(): Unit = {
    val properties = new RawMetaProperties()
    properties.version = 1
    properties.clusterId = clusterIdBase64
    assertThrows(classOf[RuntimeException], () => MetaProperties.parse(properties))
  }

  @Test
  def testMetaPropertiesWithMissingControllerId(): Unit = {
    val properties = new RawMetaProperties()
    properties.version = 1
    properties.clusterId = clusterIdBase64
    assertThrows(classOf[RuntimeException], () => MetaProperties.parse(properties))
  }

  @Test
  def testMetaPropertiesWithVersionZero(): Unit = {
    val properties = new RawMetaProperties()
    properties.version = 0
    properties.clusterId = clusterIdBase64
    properties.brokerId = 5
    val metaProps = MetaProperties.parse(properties)
    assertEquals(clusterIdBase64, metaProps.clusterId)
    assertEquals(5, metaProps.nodeId)
  }

  @Test
  def testValidMetaPropertiesWithMultipleVersionsInLogDirs(): Unit = {
    // Let's create two directories with meta.properties one in v0 and v1.
    val props1 = new RawMetaProperties()
    props1.version = 0
    props1.clusterId = clusterIdBase64
    props1.brokerId = 5
    val props2 = new RawMetaProperties()
    props2.version = 1
    props2.clusterId = clusterIdBase64
    props2.nodeId = 5
    for (ignoreMissing <- Seq(true, false)) {
      val (metaProps, offlineDirs) = getMetadataWithMultipleMetaPropLogDirs(Seq(props1, props2), ignoreMissing, kraftMode = true)
      assertEquals(MetaProperties.parse(props2), MetaProperties.parse(metaProps))
      assertEquals(Seq.empty, offlineDirs)
    }
  }

  @Test
  def testInvalidMetaPropertiesWithMultipleVersionsInLogDirs(): Unit = {
    // Let's create two directories with meta.properties one in v0 and v1.
    val props1 = new RawMetaProperties()
    props1.version = 0
    props1.brokerId = 5
    val props2 = new RawMetaProperties()
    props2.version = 1
    props2.clusterId = clusterIdBase64
    props2.nodeId = 5
    for (ignoreMissing <- Seq(true, false)) {
      assertThrows(classOf[RuntimeException],
        () => getMetadataWithMultipleMetaPropLogDirs(Seq(props1, props2), ignoreMissing, kraftMode = true))
    }
  }

  private def getMetadataWithMultipleMetaPropLogDirs(metaProperties: Seq[RawMetaProperties],
                                                     ignoreMissing: Boolean,
                                                     kraftMode: Boolean): (RawMetaProperties, collection.Seq[String]) = {
    val logDirs = mutable.Buffer[File]()
    try {
      for (mp <- metaProperties) {
        val logDir = TestUtils.tempDirectory()
        logDirs += logDir
        val propFile = new File(logDir.getAbsolutePath, "meta.properties")
        val fs = new FileOutputStream(propFile)
        try {
          mp.props.store(fs, "")
          fs.flush()
          fs.getFD.sync()
        } finally {
          Utils.closeQuietly(fs, propFile.getName)
        }
      }
      BrokerMetadataCheckpoint.getBrokerMetadataAndOfflineDirs(logDirs.map(_.getAbsolutePath), ignoreMissing, kraftMode)
    } finally {
      logDirs.foreach(logDir => CoreUtils.swallow(Utils.delete(logDir), this))
    }
  }

  @Test
  def testGetBrokerMetadataAndOfflineDirsWithNonexistentDirectories(): Unit = {
    // Use a regular file as an invalid log dir to trigger an IO error
    val invalidDir = TestUtils.tempFile("blah")
    try {
      // The `ignoreMissing` and `kraftMode` flag has no effect if there is an IO error
      testEmptyGetBrokerMetadataAndOfflineDirs(invalidDir,
        expectedOfflineDirs = Seq(invalidDir), ignoreMissing = true, kraftMode = true)
      testEmptyGetBrokerMetadataAndOfflineDirs(invalidDir,
        expectedOfflineDirs = Seq(invalidDir), ignoreMissing = false, kraftMode = true)
    } finally {
      Utils.delete(invalidDir)
    }
  }

  @Test
  def testGetBrokerMetadataAndOfflineDirsIgnoreMissing(): Unit = {
    val tempDir = TestUtils.tempDirectory()
    try {
      testEmptyGetBrokerMetadataAndOfflineDirs(tempDir,
        expectedOfflineDirs = Seq(), ignoreMissing = true, kraftMode = true)
      testEmptyGetBrokerMetadataAndOfflineDirs(tempDir,
        expectedOfflineDirs = Seq(), ignoreMissing = true, kraftMode = false)

      assertThrows(classOf[RuntimeException],
        () => BrokerMetadataCheckpoint.getBrokerMetadataAndOfflineDirs(
          Seq(tempDir.getAbsolutePath), ignoreMissing = false, kraftMode = false))
      assertThrows(classOf[RuntimeException],
        () => BrokerMetadataCheckpoint.getBrokerMetadataAndOfflineDirs(
          Seq(tempDir.getAbsolutePath), ignoreMissing = false, kraftMode = true))
    } finally {
      Utils.delete(tempDir)
    }
  }

  private def testEmptyGetBrokerMetadataAndOfflineDirs(
    logDir: File,
    expectedOfflineDirs: Seq[File],
    ignoreMissing: Boolean,
    kraftMode: Boolean
  ): Unit = {
    val (metaProperties, offlineDirs) = BrokerMetadataCheckpoint.getBrokerMetadataAndOfflineDirs(
      Seq(logDir.getAbsolutePath), ignoreMissing, kraftMode = false)
    assertEquals(expectedOfflineDirs.map(_.getAbsolutePath), offlineDirs)
    assertEquals(new Properties(), metaProperties.props)
  }

}
