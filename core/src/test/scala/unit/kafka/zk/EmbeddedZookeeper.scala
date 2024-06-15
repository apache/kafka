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

import org.apache.zookeeper.server.ZooKeeperServer
import org.apache.zookeeper.server.NIOServerCnxnFactory
import kafka.utils.{CoreUtils, Logging, TestUtils}

import java.net.InetSocketAddress
import org.apache.kafka.common.utils.Utils

import java.io.Closeable

/**
 * ZooKeeperServer wrapper that starts the server with temporary directories during construction and deletes
 * the directories when `shutdown()` is called.
 *
 * This is an internal class and it's subject to change. We recommend that you implement your own simple wrapper
 * if you need similar functionality.
 */
// This should be named EmbeddedZooKeeper for consistency with other classes, but since this is widely used by other
// projects (even though it's internal), we keep the name as it is until we have a publicly supported test library for
// others to use.
class EmbeddedZookeeper extends Closeable with Logging {

  val snapshotDir = TestUtils.tempDir()
  val logDir = TestUtils.tempDir()
  val tickTime = 800 // allow a maxSessionTimeout of 20 * 800ms = 16 secs

  System.setProperty("zookeeper.forceSync", "no")  //disable fsync to ZK txn log in tests to avoid timeout
  val zookeeper = new ZooKeeperServer(snapshotDir, logDir, tickTime)
  val factory = new NIOServerCnxnFactory()
  private val addr = new InetSocketAddress("127.0.0.1", TestUtils.RandomPort)
  factory.configure(addr, 0)
  factory.startup(zookeeper)
  val port = zookeeper.getClientPort

  def shutdown(): Unit = {
    // Also shuts down ZooKeeperServer
    CoreUtils.swallow(factory.shutdown(), this)

    def isDown(): Boolean = {
      try {
        ZkFourLetterWords.sendStat("127.0.0.1", port, 3000)
        false
      } catch { case _: Throwable => true }
    }

    Iterator.continually(isDown()).exists(identity)
    CoreUtils.swallow(zookeeper.getZKDatabase.close(), this)

    Utils.delete(logDir)
    Utils.delete(snapshotDir)
  }

  override def close(): Unit = shutdown()
}
