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

import kafka.testkit.{KafkaClusterTestKit, TestKitNodes}
import org.apache.kafka.clients.admin.Admin
import org.junit.rules.Timeout
import org.junit.{Rule, Test}

class Kip500ControllerTest {
  @Rule
  def globalTimeout = Timeout.millis(120000)

  @Test
  def testCreateControllerAndClose(): Unit = {
    val cluster = new KafkaClusterTestKit.Builder(
      new TestKitNodes.Builder().setNumControllerNodes(1).build()).build()
    try {
    } finally {
      cluster.close()
    }
  }

  @Test
  def testCreateControllersAndSendMetadataRequest(): Unit = {
    val cluster = new KafkaClusterTestKit.Builder(
      new TestKitNodes.Builder().setNumControllerNodes(3).build()).build()
    try {
      cluster.format()
      cluster.startup()
      val adminClient = Admin.create(cluster.clientProperties())
      try {
      } finally  {
        adminClient.close()
      }
    } finally {
      cluster.close()
    }
  }
}
