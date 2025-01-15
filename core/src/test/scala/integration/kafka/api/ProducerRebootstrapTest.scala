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
package kafka.api

import org.apache.kafka.clients.producer.ProducerRecord
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource

class ProducerRebootstrapTest extends RebootstrapTest {
  @ParameterizedTest(name = "{displayName}.quorum=kraft.useRebootstrapTriggerMs={0}")
  @ValueSource(booleans = Array(false, true))
  def testRebootstrap(useRebootstrapTriggerMs: Boolean): Unit = {
    // It's ok to shut the leader down, cause the reelection is small enough to the producer timeout.
    server1.shutdown()
    server1.awaitShutdown()

    val producer = createProducer(configOverrides = clientOverrides(useRebootstrapTriggerMs))

    // Only the server 0 is available for the producer during the bootstrap.
    val recordMetadata0 = producer.send(new ProducerRecord(topic, part, "key 0".getBytes, "value 0".getBytes)).get()
    assertEquals(0, recordMetadata0.offset())

    server0.shutdown()
    server0.awaitShutdown()
    server1.startup()

    // The server 0, originally cached during the bootstrap, is offline.
    // However, the server 1 from the bootstrap list is online.
    // Should be able to produce records.
    val recordMetadata1 = producer.send(new ProducerRecord(topic, part, "key 1".getBytes, "value 1".getBytes)).get()
    assertEquals(0, recordMetadata1.offset())

    server1.shutdown()
    server1.awaitShutdown()
    server0.startup()

    // The same situation, but the server 1 has gone and server 0 is back.
    val recordMetadata2 = producer.send(new ProducerRecord(topic, part, "key 1".getBytes, "value 1".getBytes)).get()
    assertEquals(1, recordMetadata2.offset())
  }
}
