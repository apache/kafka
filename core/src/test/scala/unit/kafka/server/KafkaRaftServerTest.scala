/*
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
import java.util.Properties

import kafka.common.{InconsistentBrokerIdException, InconsistentControllerIdException}
import org.apache.kafka.common.Uuid
import org.apache.kafka.common.utils.Utils
import org.apache.kafka.test.TestUtils
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test

class KafkaRaftServerTest {

  @Test
  def testSuccessfulLoadMetaProperties(): Unit = {
    testSuccessfulLoadMetaProperties(brokerId = Some(0), controllerId = None)
    testSuccessfulLoadMetaProperties(brokerId = Some(0), controllerId = Some(10))
    testSuccessfulLoadMetaProperties(brokerId = None, controllerId = Some(10))
  }

  def testSuccessfulLoadMetaProperties(
    brokerId: Option[Int],
    controllerId: Option[Int]
  ): Unit = {
    val metaProperties = MetaProperties(
      clusterId = Uuid.randomUuid(),
      brokerId = brokerId,
      controllerId = controllerId
    )

    val configProperties = new Properties
    val roles = Seq(brokerId.map(_ => "broker"), controllerId.map(_ => "controller")).flatten.mkString(",")
    configProperties.put(KafkaConfig.ProcessRolesProp, roles)
    brokerId.foreach(id => configProperties.put(KafkaConfig.BrokerIdProp, id.toString))
    controllerId.foreach(id => configProperties.put(KafkaConfig.ControllerIdProp, id.toString))

    val (loadedMetaProperties, offlineDirs) =
      invokeLoadMetaProperties(metaProperties, configProperties)

    assertEquals(metaProperties, loadedMetaProperties)
    assertEquals(Seq.empty, offlineDirs)
  }

  @Test
  def testLoadMetaPropertiesWithInconsistentBrokerId(): Unit = {
    testLoadMetaPropertiesWithInconsistentBrokerId(metaBrokerId = 1, configBrokerId = Some(0))
    testLoadMetaPropertiesWithInconsistentBrokerId(metaBrokerId = 1, configBrokerId = None)
  }

  def testLoadMetaPropertiesWithInconsistentBrokerId(
    metaBrokerId: Int,
    configBrokerId: Option[Int]
  ): Unit = {
    val metaProperties = MetaProperties(
      clusterId = Uuid.randomUuid(),
      brokerId = Some(metaBrokerId),
      controllerId = None
    )

    val configProperties = new Properties
    configProperties.put(KafkaConfig.ProcessRolesProp, "broker")
    configBrokerId.foreach(brokerId => configProperties.put(KafkaConfig.BrokerIdProp, brokerId.toString))

    assertThrows(classOf[InconsistentBrokerIdException], () =>
      invokeLoadMetaProperties(metaProperties, configProperties))
  }

  @Test
  def testLoadMetaPropertiesWithInconsistentControllerId(): Unit = {
    testLoadMetaPropertiesWithInconsistentControllerId(metaControllerId = 1, configControllerId = Some(0))
    testLoadMetaPropertiesWithInconsistentControllerId(metaControllerId = 1, configControllerId = None)
  }

  def testLoadMetaPropertiesWithInconsistentControllerId(
    metaControllerId: Int,
    configControllerId: Option[Int]
  ): Unit = {
    val metaProperties = MetaProperties(
      clusterId = Uuid.randomUuid(),
      brokerId = None,
      controllerId = Some(metaControllerId)
    )

    val configProperties = new Properties

    configProperties.put(KafkaConfig.ProcessRolesProp, "controller")
    configControllerId.foreach(brokerId => configProperties.put(KafkaConfig.ControllerIdProp, brokerId.toString))

    assertThrows(classOf[InconsistentControllerIdException], () =>
      invokeLoadMetaProperties(metaProperties, configProperties))
  }

  private def invokeLoadMetaProperties(
    metaProperties: MetaProperties,
    configProperties: Properties
  ): (MetaProperties, Seq[String]) = {
    val tempLogDir = TestUtils.tempDirectory()
    try {
      writeMetaProperties(tempLogDir, metaProperties)

      configProperties.put(KafkaConfig.LogDirProp, tempLogDir.getAbsolutePath)
      val config = KafkaConfig.fromProps(configProperties)
      KafkaRaftServer.loadMetaProperties(config)
    } finally {
      Utils.delete(tempLogDir)
    }
  }

  private def writeMetaProperties(
    logDir: File,
    metaProperties: MetaProperties
  ): Unit = {
    val metaPropertiesFile = new File(logDir.getAbsolutePath + File.separator + "meta.properties")
    val checkpoint = new BrokerMetadataCheckpoint(metaPropertiesFile)
    checkpoint.write(metaProperties.toProperties)
  }

}
