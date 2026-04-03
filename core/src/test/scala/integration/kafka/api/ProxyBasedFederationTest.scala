/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing permissions and limitations under the
 * License.
 */

package kafka.api

import org.junit.jupiter.api.Test

import scala.collection.JavaConverters._

/**
 * Currently a simple proof of concept of a multi-cluster integration test, but ultimately intended
 * as a feasibility test for proxy-based federation:  specifically, can a vanilla client (whether
 * consumer, producer, or admin) talk to a federated set of two or more physical clusters and
 * successfully execute all parts of its API?
 */
class ProxyBasedFederationTest extends MultiClusterAbstractConsumerTest {
  override def numClusters: Int = 2  // need one ZK instance for each Kafka cluster  [TODO: can we "chroot" instead?]
  override def brokerCountPerCluster: Int = 3  // three _per Kafka cluster_, i.e., six total

  @Test
  def testBasicMultiClusterSetup(): Unit = {
    val numRecords = 1000
    val producer = createProducer()
    sendRecords(producer, numRecords, tp1c0)

    val consumer = createConsumer()
    consumer.assign(List(tp1c0).asJava)
    consumer.seek(tp1c0, 0)
    consumeAndVerifyRecords(consumer = consumer, numRecords = numRecords, startingOffset = 0)
  }

}
