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
package kafka.raft

import java.util.concurrent.CompletableFuture

import kafka.raft.KafkaRaftManager.RaftIoThread
import org.apache.kafka.raft.KafkaRaftClient
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test
import org.mockito.Mockito._

class RaftManagerTest {

  @Test
  def testShutdownIoThread(): Unit = {
    val raftClient = mock(classOf[KafkaRaftClient[String]])
    val ioThread = new RaftIoThread(raftClient, threadNamePrefix = "test-raft")

    when(raftClient.isRunning).thenReturn(true)
    assertTrue(ioThread.isRunning)

    val shutdownFuture = new CompletableFuture[Void]
    when(raftClient.shutdown(5000)).thenReturn(shutdownFuture)

    ioThread.initiateShutdown()
    assertTrue(ioThread.isRunning)
    assertTrue(ioThread.isShutdownInitiated)
    verify(raftClient).shutdown(5000)

    shutdownFuture.complete(null)
    when(raftClient.isRunning).thenReturn(false)
    ioThread.run()
    assertFalse(ioThread.isRunning)
    assertTrue(ioThread.isShutdownComplete)
  }

  @Test
  def testUncaughtExceptionInIoThread(): Unit = {
    val raftClient = mock(classOf[KafkaRaftClient[String]])
    val ioThread = new RaftIoThread(raftClient, threadNamePrefix = "test-raft")

    when(raftClient.isRunning).thenReturn(true)
    assertTrue(ioThread.isRunning)

    when(raftClient.poll()).thenThrow(new RuntimeException)
    ioThread.run()

    assertTrue(ioThread.isShutdownComplete)
    assertTrue(ioThread.isThreadFailed)
    assertFalse(ioThread.isRunning)
  }

}
