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

package kafka.server.metadata

import java.util.Collections
import kafka.utils.TestUtils
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.{Test, Timeout}
import org.slf4j.LoggerFactory

import java.util.concurrent.TimeUnit
import scala.collection.mutable


@Timeout(value = 120000, unit = TimeUnit.MILLISECONDS)
class MetadataBrokersTest {

  private val log = LoggerFactory.getLogger(classOf[MetadataBrokersTest])

  val emptyBrokers = new MetadataBrokers(Collections.emptyList(), Collections.emptyMap())

  @Test
  def testBuildBrokers(): Unit = {
    val builder = new MetadataBrokersBuilder(log, emptyBrokers)
    builder.add(TestUtils.createMetadataBroker(0))
    builder.add(TestUtils.createMetadataBroker(1))
    builder.add(TestUtils.createMetadataBroker(2))
    builder.add(TestUtils.createMetadataBroker(3))
    builder.remove(0)
    val brokers = builder.build()
    val found = new mutable.HashSet[MetadataBroker]
    brokers.iterator().foreach { found += _ }
    val expected = new mutable.HashSet[MetadataBroker]
    expected += TestUtils.createMetadataBroker(1)
    expected += TestUtils.createMetadataBroker(2)
    expected += TestUtils.createMetadataBroker(3)
    assertEquals(expected, found)
  }

  @Test
  def testChangeFencing(): Unit = {
    val builder = new MetadataBrokersBuilder(log, emptyBrokers)
    assertEquals(None, builder.get(0))
    assertThrows(classOf[RuntimeException], () => builder.changeFencing(0, false))
    builder.add(TestUtils.createMetadataBroker(0, fenced = true))
    assertTrue(builder.get(0).get.fenced)
    builder.changeFencing(0, false)
    assertFalse(builder.get(0).get.fenced)
    val brokers = builder.build()
    assertTrue(brokers.aliveBroker(0).isDefined)
  }

  @Test
  def testAliveBrokers(): Unit = {
    val builder = new MetadataBrokersBuilder(log, emptyBrokers)
    builder.add(TestUtils.createMetadataBroker(0))
    builder.add(TestUtils.createMetadataBroker(1))
    builder.add(TestUtils.createMetadataBroker(2))
    builder.changeFencing(1, true)
    val brokers = builder.build()
    assertEquals(2, brokers.aliveBrokers().size)
    assertTrue(brokers.aliveBrokers().exists(_.id == 0))
    assertTrue(!brokers.aliveBrokers().exists(_.id == 1))
    assertTrue(brokers.aliveBrokers().exists(_.id == 2))
    while (!brokers.randomAliveBrokerId().contains(0)) { }
    while (!brokers.randomAliveBrokerId().contains(2)) { }
    assertEquals(3, brokers.size())
    assertEquals(Some(TestUtils.createMetadataBroker(0)), brokers.get(0))
    assertEquals(Some(TestUtils.createMetadataBroker(1, fenced = true)), brokers.get(1))
    assertEquals(Some(TestUtils.createMetadataBroker(2)), brokers.get(2))
    assertEquals(None, brokers.get(3))
    assertEquals(Some(TestUtils.createMetadataBroker(0)), brokers.aliveBroker(0))
    assertEquals(None, brokers.aliveBroker(1))
  }
}
