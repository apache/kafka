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

import kafka.admin.ConfigCommand.ConfigCommandOptions
import org.junit.Assert._
import org.junit.Test
import kafka.utils.Logging
import kafka.zk.ZooKeeperTestHarness

class ConfigCommandTest extends ZooKeeperTestHarness with Logging {
  @Test
  def testArgumentParse() {
    // Should parse correctly
    var createOpts = new ConfigCommandOptions(Array("--zookeeper", zkConnect,
                                                     "--entity-name", "x",
                                                     "--entity-type", "clients",
                                                     "--describe"))
    createOpts.checkArgs()

    // For --alter and added config
    createOpts = new ConfigCommandOptions(Array("--zookeeper", zkConnect,
                                                "--entity-name", "x",
                                                "--entity-type", "clients",
                                                "--alter",
                                                "--add-config", "a=b,c=d"))
    createOpts.checkArgs()

    // For alter and deleted config
    createOpts = new ConfigCommandOptions(Array("--zookeeper", zkConnect,
                                                "--entity-name", "x",
                                                "--entity-type", "clients",
                                                "--alter",
                                                "--delete-config", "a,b,c"))
    createOpts.checkArgs()

    // For alter and both added, deleted config
    createOpts = new ConfigCommandOptions(Array("--zookeeper", zkConnect,
                                                "--entity-name", "x",
                                                "--entity-type", "clients",
                                                "--alter",
                                                "--add-config", "a=b,c=d",
                                                "--delete-config", "a"))
    createOpts.checkArgs()
    val addedProps = ConfigCommand.parseConfigsToBeAdded(createOpts)
    assertEquals(2, addedProps.size())
    assertEquals("b", addedProps.getProperty("a"))
    assertEquals("d", addedProps.getProperty("c"))

    val deletedProps = ConfigCommand.parseConfigsToBeDeleted(createOpts)
    assertEquals(1, deletedProps.size)
    assertEquals("a", deletedProps(0))
  }
}
