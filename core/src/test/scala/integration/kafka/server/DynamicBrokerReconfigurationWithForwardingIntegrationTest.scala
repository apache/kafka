/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.server

import java.util.Properties

import org.junit.Test

/**
 * Integration test suite for forwarding mechanism applied on AlterConfigs.
 * This class basically reused everything from {@link DynamicBrokerReconfigurationTest}
 * with the KIP-500 mode enabled for trust store alter test.
 */
class DynamicBrokerReconfigurationWithForwardingIntegrationTest extends DynamicBrokerReconfigurationTest {

  override def addExtraProps(props: Properties): Unit = {
    props.put(KafkaConfig.EnableMetadataQuorumProp, true)
  }

  @Test
  override def testConfigDescribeUsingAdminClient(): Unit = {
  }

  @Test
  override def testUpdatesUsingConfigProvider(): Unit = {
  }

  @Test
  override def testLogCleanerConfig(): Unit = {
  }

  @Test
  override def testTrustStoreAlter(): Unit = {
    super.testTrustStoreAlter()
  }

  @Test
  override def testConsecutiveConfigChange(): Unit ={
  }

  @Test
  override def testDefaultTopicConfig(): Unit = {
  }

  @Test
  override def testUncleanLeaderElectionEnable(): Unit = {
  }

  @Test
  override def testThreadPoolResize(): Unit = {
  }

  @Test
  override def testMetricsReporterUpdate(): Unit = {
  }

  @Test
  override def testAdvertisedListenerUpdate(): Unit = {
  }

  @Test
  override def testAddRemoveSaslListeners(): Unit = {
  }
}
