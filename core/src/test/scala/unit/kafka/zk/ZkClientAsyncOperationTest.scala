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

package kafka.zk

import kafka.utils.{ZkReadCallback, TestUtils}
import org.apache.zookeeper.data.Stat
import org.junit.Test
import org.junit.Assert._

class ZkClientAsyncOperationTest extends ZooKeeperTestHarness {

  @Test
  def testAsyncWrite() {
    val numPaths = 1000
    val paths = (0 until numPaths).map { i => {
      val path = "/testPath" + i
      zkUtils.createPersistentPath(path, i.toString)
      val (dataOpt, stat) = zkUtils.readDataMaybeNull(path)
      assertEquals(i.toString, dataOpt.get)
      val zkVersion = stat.getVersion
      assertEquals(0, zkVersion)
      path
    }}

    var callbackFired = 0
    def handleResult(updateSucceeded: Boolean, newZkVersion: Int) = {
      assertTrue(updateSucceeded)
      assertEquals(1, newZkVersion)
      callbackFired += 1
    }
    paths.foreach { path =>
      zkUtils.asyncConditionalUpdatePersistentPath(path, "-1", 0, handleResult)
    }
    TestUtils.waitUntilTrue(() => callbackFired == numPaths, "Callback did not fire before timeout.")

    paths.foreach { path =>
      val (newData, newStat) = zkUtils.readDataMaybeNull(path)
      assertEquals("-1", newData.get)
      assertEquals(1, newStat.getVersion)
    }
  }

  @Test
  def testAsyncRead() {
    val numPaths = 1000
    val paths = (0 until numPaths).map { i => {
      val path = "/testPath" + i
      zkUtils.createPersistentPath(path, i.toString)
      val (dataOpt, stat) = zkUtils.readDataMaybeNull(path)
      assertEquals(i.toString, dataOpt.get)
      val zkVersion = stat.getVersion
      assertEquals(0, zkVersion)
      path
    }}

    var writeCallbackFired = 0
    def handleWriteResult(updateSucceeded: Boolean, newZkVersion: Int) = {
      assertTrue(updateSucceeded)
      assertEquals(1, newZkVersion)
      writeCallbackFired += 1
    }
    paths.foreach { path =>
      zkUtils.asyncConditionalUpdatePersistentPath(path, path + "data", 0, handleWriteResult)
    }
    TestUtils.waitUntilTrue(() => writeCallbackFired == numPaths, "Callback did not fire before timeout.")

    var readCallbackFired = 0
    paths.foreach { path => {
      zkUtils.asyncReadDataMaybeNull(path, new ZkReadCallback() {
        override def handle(dataOpt: Option[String], stat: Stat, exceptionOpt: Option[Exception]) = {
          assertTrue(exceptionOpt.isEmpty)
          assertEquals(path + "data", dataOpt.get)
          assertEquals(1, stat.getVersion)
          readCallbackFired += 1
        }
      })
    }}
    TestUtils.waitUntilTrue(() => readCallbackFired == numPaths, "Callback did not fire before timeout.")
  }

}
