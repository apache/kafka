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

import java.lang.reflect.{Constructor, Field}
import java.net.InetSocketAddress
import java.util.concurrent.CountDownLatch

import kafka.utils.{CoreUtils, TestUtils}
import org.apache.kafka.common.utils.Utils
import org.apache.zookeeper.server.{NIOServerCnxnFactory, ZooKeeperServer}

import scala.util.Try

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
class EmbeddedZookeeper() {

  val snapshotDir = TestUtils.tempDir()
  val logDir = TestUtils.tempDir()
  val tickTime = 500
  val zookeeper = new ZooKeeperServer(snapshotDir, logDir, tickTime)

  disableZKShutdownHandlerIsNotRegisteredError(zookeeper)

  val factory = new NIOServerCnxnFactory()
  private val addr = new InetSocketAddress("127.0.0.1", TestUtils.RandomPort)
  factory.configure(addr, 0)
  factory.startup(zookeeper)
  val port = zookeeper.getClientPort

  def shutdown() {
    CoreUtils.swallow(zookeeper.shutdown())
    CoreUtils.swallow(factory.shutdown())

    def isDown(): Boolean = {
      try {
        ZkFourLetterWords.sendStat("127.0.0.1", port, 3000)
        false
      } catch { case _: Throwable => true }
    }

    Iterator.continually(isDown()).exists(identity)

    Utils.delete(logDir)
    Utils.delete(snapshotDir)
  }

  /**
    * There are some useless log appeared when running a lot of tests:
    *
    *   ERROR ZKShutdownHandler is not registered, so ZooKeeper server won't take any action on ERROR or SHUTDOWN server state changes (org.apache.zookeeper.server.ZooKeeperServer:472)
    *   ERROR ZKShutdownHandler is not registered, so ZooKeeper server won't take any action on ERROR or SHUTDOWN server state changes (org.apache.zookeeper.server.ZooKeeperServer:472)
    *
    * To disable such logs this method is used.
    *
    * If the API of ZooKeeperServer will be changed the method will not cause tests to fail because
    * the method is wrapped with [[Try]]
    *
    * @param zooKeeperServer instance of [[ZooKeeperServer]] used for testing
    * @return attempt to disable
    */
  def disableZKShutdownHandlerIsNotRegisteredError(zooKeeperServer: ZooKeeperServer): Try[Unit] = {
    Try {
      val zkShutdownHandlerField: Field = zooKeeperServer.getClass.getDeclaredField("zkShutdownHandler")
      val clazz: Class[_] = Class.forName("org.apache.zookeeper.server.ZooKeeperServerShutdownHandler")
      val constructor: Constructor[_] = clazz.getDeclaredConstructor(classOf[CountDownLatch])

      // make filed and constructor constructor accessible
      constructor.setAccessible(true)
      zkShutdownHandlerField.setAccessible(true)

      // new ZooKeeperServerShutdownHandler(new CountDownLatch(1))
      val zkShutdownHandler = constructor.newInstance(new CountDownLatch(1))

      // ZKServer.zkShutdownHandler = new ZooKeeperServerShutdownHandler(new CountDownLatch(1))
      zkShutdownHandlerField.set(zooKeeperServer, zkShutdownHandler)
    }
  }
  
}
