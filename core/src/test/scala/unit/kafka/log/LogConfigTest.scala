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

package kafka.log

import java.util.Properties

import kafka.server.{ThrottledReplicaListValidator, KafkaConfig, KafkaServer}
import kafka.utils.TestUtils
import org.apache.kafka.common.config.ConfigException
import org.junit.{Assert, Test}
import org.junit.Assert._
import org.scalatest.Assertions._

class LogConfigTest {

  /** 
   * This test verifies that KafkaConfig object initialization does not depend on 
   * LogConfig initialization. Bad things happen due to static initialization 
   * order dependencies. For example, LogConfig.configDef ends up adding null 
   * values in serverDefaultConfigNames. This test ensures that the mapping of 
   * keys from LogConfig to KafkaConfig are not missing values.
   */
  @Test
  def ensureNoStaticInitializationOrderDependency() {
    // Access any KafkaConfig val to load KafkaConfig object before LogConfig.
    assertTrue(KafkaConfig.LogRetentionTimeMillisProp != null)
    assertTrue(LogConfig.configNames.forall { config =>
      val serverConfigOpt = LogConfig.serverConfigName(config)
      serverConfigOpt.isDefined && (serverConfigOpt.get != null)
    })
  }

  @Test
  def testKafkaConfigToProps() {
    val millisInHour = 60L * 60L * 1000L
    val kafkaProps = TestUtils.createBrokerConfig(nodeId = 0, zkConnect = "")
    kafkaProps.put(KafkaConfig.LogRollTimeHoursProp, "2")
    kafkaProps.put(KafkaConfig.LogRollTimeJitterHoursProp, "2")
    kafkaProps.put(KafkaConfig.LogRetentionTimeHoursProp, "2")

    val kafkaConfig = KafkaConfig.fromProps(kafkaProps)
    val logProps = KafkaServer.copyKafkaConfigToLog(kafkaConfig)
    assertEquals(2 * millisInHour, logProps.get(LogConfig.SegmentMsProp))
    assertEquals(2 * millisInHour, logProps.get(LogConfig.SegmentJitterMsProp))
    assertEquals(2 * millisInHour, logProps.get(LogConfig.RetentionMsProp))
  }

  @Test
  def testFromPropsEmpty() {
    val p = new Properties()
    val config = LogConfig(p)
    Assert.assertEquals(LogConfig(), config)
  }

  @Test
  def testFromPropsInvalid() {
    LogConfig.configNames.foreach(name => name match {
      case LogConfig.UncleanLeaderElectionEnableProp => assertPropertyInvalid(name, "not a boolean")
      case LogConfig.RetentionBytesProp => assertPropertyInvalid(name, "not_a_number")
      case LogConfig.RetentionMsProp => assertPropertyInvalid(name, "not_a_number" )
      case LogConfig.CleanupPolicyProp => assertPropertyInvalid(name, "true", "foobar")
      case LogConfig.MinCleanableDirtyRatioProp => assertPropertyInvalid(name, "not_a_number", "-0.1", "1.2")
      case LogConfig.MinInSyncReplicasProp => assertPropertyInvalid(name, "not_a_number", "0", "-1")
      case LogConfig.MessageFormatVersionProp => assertPropertyInvalid(name, "")
      case _ => assertPropertyInvalid(name, "not_a_number", "-1")
    })
  }

  @Test
  def shouldValidateThrottledReplicasConfig() {
    assertTrue(isValid("*"))
    assertTrue(isValid("* "))
    assertTrue(isValid(""))
    assertTrue(isValid(" "))
    assertTrue(isValid("100:10"))
    assertTrue(isValid("100:10,12:10"))
    assertTrue(isValid("100:10,12:10,15:1"))
    assertTrue(isValid("100:10,12:10,15:1  "))
    assertTrue(isValid("100:0,"))

    assertFalse(isValid("100"))
    assertFalse(isValid("100:"))
    assertFalse(isValid("100:0,10"))
    assertFalse(isValid("100:0,10:"))
    assertFalse(isValid("100:0,10:   "))
    assertFalse(isValid("100 :0,10:   "))
    assertFalse(isValid("100: 0,10:   "))
    assertFalse(isValid("100:0,10 :   "))
  }

  private def isValid(configValue: String): Boolean = {
    try {
      ThrottledReplicaListValidator.ensureValidString("", configValue)
      true
    } catch {
      case _: ConfigException => false
    }
  }

  private def assertPropertyInvalid(name: String, values: AnyRef*) {
    values.foreach((value) => {
      val props = new Properties
      props.setProperty(name, value.toString)
      intercept[Exception] {
        LogConfig(props)
      }
    })
  }

}
