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

package unit.kafka.zk

import java.util.concurrent.CountDownLatch

import org.junit.Assert.assertEquals

import kafka.utils.{ZKLock, ZkUtils}
import kafka.zk.ZooKeeperTestHarness
import java.util.concurrent.atomic.AtomicInteger

import org.junit.Assert._
import org.junit.{Test, Before}

class ZKLockTest extends ZooKeeperTestHarness {

  val lockPath: String = "/lock_dir"
  val zkSessionTimeoutMs = 1000
  val zkConnectionTimeoutMs = 3000

  @Test
  def testExclusiveZKLock {

    val zkClient = ZkUtils.createZkClient(zkConnect, zkSessionTimeoutMs, zkConnectionTimeoutMs)
    
    ZkUtils.createPersistentPath(zkClient, lockPath)

    assertTrue(ZkUtils.pathExists(zkClient, lockPath))

    val threads = 10
    var update = 0
    var notUpdated = 0
    val lockHolders = new AtomicInteger();
    val startBarrier = new CountDownLatch(1)
    val finishBarrier = new CountDownLatch(threads)

    val task = new Runnable () {
      override def run {

        startBarrier.await() // wait for all threads to be started
        val zkLock = new ZKLock(zkClient, lockPath)
        try {
          zkLock.acquire()
          assertEquals(1, lockHolders.incrementAndGet())
          if (update == 0)
            update += 1
          else
            notUpdated += 1
        }
        catch {
          case e: Throwable => fail(e.getMessage)
        }
        finally {
          finishBarrier.countDown()
          assertEquals(0, lockHolders.decrementAndGet())
          zkLock.release()
        }
      }
    }

    for (i <- 1 to threads)
      new Thread(task).start()

    startBarrier.countDown() // start threads
    finishBarrier.await() // wait for all threads to finish

    assertEquals(0, ZkUtils.getChildren(zkClient, lockPath).size)

    assertEquals(1, update)
    assertEquals(9, notUpdated)

    zkClient.close()
  }
}
