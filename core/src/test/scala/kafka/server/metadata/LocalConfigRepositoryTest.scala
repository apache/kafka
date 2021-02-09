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

import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.{Test, Timeout}

import java.util.Properties
import java.util.concurrent.TimeUnit


@Timeout(value = 120000, unit = TimeUnit.MILLISECONDS)
class LocalConfigRepositoryTest {
  @Test
  def testEmptyRepository(): Unit = {
    val repository = new LocalConfigRepository()
    assertEquals(new Properties(), repository.brokerConfig(0))
    assertEquals(new Properties(), repository.topicConfig("foo"))
  }

  @Test
  def testSetConfig(): Unit = {
    val repository = new LocalConfigRepository()
    repository.setBrokerConfig(0, "foo", null)
    assertEquals(new Properties(), repository.brokerConfig(0))

    repository.setBrokerConfig(1, "foo", "bar")
    val brokerProperties = new Properties()
    brokerProperties.put("foo", "bar")
    assertEquals(brokerProperties, repository.brokerConfig(1))

    val brokerProperties2 = new Properties()
    brokerProperties2.put("foo", "bar")
    brokerProperties2.put("foo2", "baz")
    repository.setBrokerConfig(1, "foo2", "baz")
    assertEquals(brokerProperties2, repository.brokerConfig(1))

    repository.setBrokerConfig(1, "foo2", null)
    assertEquals(brokerProperties, repository.brokerConfig(1))
  }
}
