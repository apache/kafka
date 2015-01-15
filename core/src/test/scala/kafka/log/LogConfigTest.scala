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

import org.apache.kafka.common.config.ConfigException
import org.scalatest.junit.JUnit3Suite
import org.junit.{Assert, Test}
import java.util.Properties

class LogConfigTest extends JUnit3Suite {

  @Test
  def testFromPropsDefaults() {
    val defaults = new Properties()
    defaults.put(LogConfig.SegmentBytesProp, "4242")
    val props = new Properties(defaults)

    val config = LogConfig.fromProps(props)

    Assert.assertEquals(4242, config.segmentSize)
    Assert.assertEquals("LogConfig defaults should be retained", Defaults.MaxMessageSize, config.maxMessageSize)
    Assert.assertEquals("producer", config.compressionType)
  }

  @Test
  def testFromPropsEmpty() {
    val p = new Properties()
    val config = LogConfig.fromProps(p)
    Assert.assertEquals(LogConfig(), config)
  }

  @Test
  def testFromPropsToProps() {
    import scala.util.Random._
    val expected = new Properties()
    LogConfig.configNames().foreach((name) => {
      name match {
        case LogConfig.UncleanLeaderElectionEnableProp => expected.setProperty(name, randFrom("true", "false"))
        case LogConfig.CompressionTypeProp => expected.setProperty(name, randFrom("producer", "uncompressed", "gzip"))
        case LogConfig.CleanupPolicyProp => expected.setProperty(name, randFrom(LogConfig.Compact, LogConfig.Delete))
        case LogConfig.MinCleanableDirtyRatioProp => expected.setProperty(name, "%.1f".format(nextDouble * .9 + .1))
        case LogConfig.MinInSyncReplicasProp => expected.setProperty(name, (nextInt(Int.MaxValue - 1) + 1).toString)
        case LogConfig.RetentionBytesProp => expected.setProperty(name, nextInt().toString)
        case positiveIntProperty => expected.setProperty(name, nextInt(Int.MaxValue).toString)
      }
    })

    val actual = LogConfig.fromProps(expected).toProps
    Assert.assertEquals(expected, actual)
  }

  @Test
  def testFromPropsInvalid() {
    LogConfig.configNames().foreach((name) => {
      name match {
        case LogConfig.UncleanLeaderElectionEnableProp  => return
        case LogConfig.RetentionBytesProp => assertPropertyInvalid(name, "not_a_number")
        case LogConfig.CleanupPolicyProp => assertPropertyInvalid(name, "true", "foobar");
        case LogConfig.MinCleanableDirtyRatioProp => assertPropertyInvalid(name, "not_a_number", "-0.1", "1.2")
        case LogConfig.MinInSyncReplicasProp => assertPropertyInvalid(name, "not_a_number", "0", "-1")
        case positiveIntProperty => assertPropertyInvalid(name, "not_a_number", "-1")
      }
    })
   }

  private def assertPropertyInvalid(name: String, values: AnyRef*) {
    values.foreach((value) => {
      val props = new Properties
      props.setProperty(name, value.toString)
      intercept[ConfigException] {
        LogConfig.fromProps(props)
      }
    })
  }

  private def randFrom[T](choices: T*): T = {
    import scala.util.Random
    choices(Random.nextInt(choices.size))
  }
}
