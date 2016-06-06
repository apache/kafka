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

package unit.kafka.utils

import java.util.UUID

import kafka.zk.ZooKeeperTestHarness
import org.junit.Assert._
import org.junit.Test

class ZkUtilsTest extends ZooKeeperTestHarness {

  @Test
  def testSuccessfulConditionalDeletePath() {
    // Given an existing path
    val randomPath = "/" + UUID.randomUUID()
    zkUtils.createPersistentPath(randomPath)
    val (_, statAfterCreation) = zkUtils.readData(randomPath)

    // Deletion is successful when the version number matches
    assertTrue("Deletion should be successful", zkUtils.conditionalDeletePath(randomPath, statAfterCreation.getVersion()))
    val (optionalData, _) = zkUtils.readDataMaybeNull(randomPath)
    assertTrue("Node should be deleted", optionalData.isEmpty)

    // Deletion is successful when the node does not exist too
    assertTrue("Deletion should be successful", zkUtils.conditionalDeletePath(randomPath, 0))
  }

  @Test
  def testAbortedConditionalDeletePath() {
    // Given an existing path that gets updated
    val randomPath = "/" + UUID.randomUUID()
    zkUtils.createPersistentPath(randomPath)
    val (_, statAfterCreation) = zkUtils.readData(randomPath)
    zkUtils.updatePersistentPath(randomPath, "data")

    // Deletion is aborted when the version number does not match
    assertFalse("Deletion should be aborted", zkUtils.conditionalDeletePath(randomPath, statAfterCreation.getVersion))
    val (optionalData, _) = zkUtils.readDataMaybeNull(randomPath)
    assertTrue("Node should still be there", optionalData.isDefined)
  }
}
