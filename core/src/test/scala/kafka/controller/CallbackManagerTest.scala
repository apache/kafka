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

package kafka.controller

import java.util.concurrent.CompletableFuture
import java.util.concurrent.atomic.AtomicBoolean

import kafka.utils.Logging
import org.junit.{Assert, Rule, Test}

class CallbackManagerTest extends Logging {
  @Rule
  def globalTimeout = org.junit.rules.Timeout.millis(120000)

  @Test
  def testCreateAndShutdown(): Unit = {
    val callbackManager = new CallbackManager("testCallbackManager")
    callbackManager.shutdown()
  }

  @Test
  def testCreateStartAndShutdown(): Unit = {
    val callbackManager = new CallbackManager("testCallbackManager")
    callbackManager.startup()
    callbackManager.shutdown()
  }

  @Test
  def testTimeouts(): Unit = {
    val callbackManager = new CallbackManager("testCallbackManager")
    try {
      callbackManager.startup()
      val future1 = new CompletableFuture[Unit]()
      val timeout1 = callbackManager.
        registerTimeout("TestTimeout1", 1, () => future1.complete(null))
      val ran2 = new AtomicBoolean(false)
      val timeout2 = callbackManager.
        registerTimeout("TestTimeout2", Integer.MAX_VALUE, () => ran2.set(true))
      future1.get()
      Assert.assertFalse(timeout1.cancel())
      Assert.assertFalse(ran2.get())
      Assert.assertTrue(timeout2.cancel())
      Assert.assertFalse(timeout2.cancel())
      Assert.assertFalse(ran2.get())
    } finally {
      callbackManager.shutdown()
    }
  }

  @Test
  def testImmediateTimeout(): Unit = {
    val callbackManager = new CallbackManager("testCallbackManager")
    try {
      callbackManager.startup()
      val future1 = new CompletableFuture[Unit]()
      val timeout1 = callbackManager.
        registerTimeout("TestTimeout1", 0, () => future1.complete(null))
      val future2 = new CompletableFuture[Unit]()
      val timeout2 = callbackManager.
        registerTimeout("TestTimeout2", 0, () => future2.complete(null))
      future1.get()
      future2.get()
      Assert.assertFalse(timeout1.cancel())
      Assert.assertFalse(timeout2.cancel())
    } finally {
      callbackManager.shutdown()
    }
  }

  @Test
  def testShutdownWithPendingTimeouts(): Unit = {
    val callbackManager = new CallbackManager("testCallbackManager")
    val ran1 = new AtomicBoolean(false)
    try {
      callbackManager.startup()
      callbackManager.registerTimeout("TestTimeout1", Integer.MAX_VALUE, () => ran1.set(true))
    } finally {
      callbackManager.shutdown()
    }
    Assert.assertFalse(ran1.get())
  }
}
