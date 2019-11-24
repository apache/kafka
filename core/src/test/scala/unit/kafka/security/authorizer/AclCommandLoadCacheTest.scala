/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package unit.kafka.security.authorizer

import java.util.Properties

import kafka.admin.AclCommand
import kafka.security.authorizer.AclAuthorizer
import kafka.server.KafkaConfig
import kafka.utils.TestUtils
import kafka.zk.ZooKeeperTestHarness
import org.junit.{Before, Test}

class AclCommandLoadCacheTest extends ZooKeeperTestHarness {

  private val testUsers1 = "User:test1"
  private val testUsers2 = "User:test2"
  private val topic = "test.topic1"
  private val notLoadCache = "false"

  /** every pull request run all test case,it may run some diff erro,we can improve this,I think*/
  @Test
  def notLoadCacheTest(): Unit = {
    AclCommand.main(Array[String]("--authorizer-properties", "zookeeper.connect=" + zkConnect, "--force", "--add", "--allow-principal", testUsers1, "--operation", "read", "--topic", topic, "--load-acl-cache", notLoadCache))
    AclCommand.main(Array[String]("--authorizer-properties", "zookeeper.connect=" + zkConnect, "--force", "--remove", "--allow-principal", testUsers1, "--operation", "read", "--topic", topic, "--load-acl-cache", notLoadCache))
  }

  @Test
  def loadCacheTest(): Unit = {
    AclCommand.main(Array[String]("--authorizer-properties", "zookeeper.connect=" + zkConnect, "--force", "--add", "--allow-principal", testUsers2, "--operation", "read", "--topic", topic))
    AclCommand.main(Array[String]("--authorizer-properties", "zookeeper.connect=" + zkConnect, "--force", "--remove", "--allow-principal", testUsers2, "--operation", "read", "--topic", topic))
  }
}