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
package kafka.admin

import joptsimple.OptionException
import org.junit.Test
import kafka.utils.TestUtils

class ListConsumerGroupTest extends ConsumerGroupCommandTest {

  @Test
  def testListConsumerGroups() {
    val simpleGroup = "simple-group"
    addSimpleGroupExecutor(group = simpleGroup)
    addConsumerGroupExecutor(numConsumers = 1)

    val cgcArgs = Array("--bootstrap-server", brokerList, "--list")
    val service = getConsumerGroupService(cgcArgs)

    val expectedGroups = Set(group, simpleGroup)
    var foundGroups = Set.empty[String]
    TestUtils.waitUntilTrue(() => {
      foundGroups = service.listGroups().toSet
      expectedGroups == foundGroups
    }, s"Expected --list to show groups $expectedGroups, but found $foundGroups.", maxRetries = 3)
  }

  @Test(expected = classOf[OptionException])
  def testListWithUnrecognizedNewConsumerOption() {
    val cgcArgs = Array("--new-consumer", "--bootstrap-server", brokerList, "--list")
    getConsumerGroupService(cgcArgs)
  }
}
