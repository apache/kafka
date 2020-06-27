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

package kafka.utils

import java.io.IOException

import org.junit.Assert.assertEquals
import org.junit.Test
import org.scalatest.Assertions.intercept

class ExitTest {
  @Test
  def shouldHaltImmediately(): Unit = {
    val array:Array[Any] = Array("a", "b")
    def haltProcedure(exitStatus: Int, message: Option[String]) : Nothing = {
      array(0) = exitStatus
      array(1) = message
      throw new IOException()
    }
    Exit.setHaltProcedure(haltProcedure)
    val statusCode = 0
    val message = Some("message")
    try {
      intercept[IOException] {Exit.halt(statusCode)}
      assertEquals(statusCode, array(0))
      assertEquals(None, array(1))

      intercept[IOException] {Exit.halt(statusCode, message)}
      assertEquals(statusCode, array(0))
      assertEquals(message, array(1))
    } finally {
      Exit.resetHaltProcedure()
    }
  }

  @Test
  def shouldExitImmediately(): Unit = {
    val array:Array[Any] = Array("a", "b")
    def exitProcedure(exitStatus: Int, message: Option[String]) : Nothing = {
      array(0) = exitStatus
      array(1) = message
      throw new IOException()
    }
    Exit.setExitProcedure(exitProcedure)
    val statusCode = 0
    val message = Some("message")
    try {
      intercept[IOException] {Exit.exit(statusCode)}
      assertEquals(statusCode, array(0))
      assertEquals(None, array(1))

      intercept[IOException] {Exit.exit(statusCode, message)}
      assertEquals(statusCode, array(0))
      assertEquals(message, array(1))
    } finally {
      Exit.resetExitProcedure()
    }
  }

  @Test
  def shouldAddShutdownHookImmediately(): Unit = {
    val name = "name"
    val array:Array[Any] = Array("", 0)
    // immediately invoke the shutdown hook to mutate the data when a hook is added
    def shutdownHookAdder(name: String, shutdownHook: => Unit) : Unit = {
      // mutate the first element
      array(0) = array(0).toString + name
      // invoke the shutdown hook (see below, it mutates the second element)
      shutdownHook
    }
    Exit.setShutdownHookAdder(shutdownHookAdder)
    def sideEffect(): Unit = {
      // mutate the second element
      array(1) = array(1).asInstanceOf[Int] + 1
    }
    try {
      Exit.addShutdownHook(name, sideEffect()) // by-name parameter, only invoked due to above shutdownHookAdder
      assertEquals(1, array(1))
      assertEquals(name * array(1).asInstanceOf[Int], array(0).toString)
      Exit.addShutdownHook(name, array(1) = array(1).asInstanceOf[Int] + 1) // by-name parameter, only invoked due to above shutdownHookAdder
      assertEquals(2, array(1))
      assertEquals(name * array(1).asInstanceOf[Int], array(0).toString)
    } finally {
      Exit.resetShutdownHookAdder()
    }
  }

  @Test
  def shouldNotInvokeShutdownHookImmediately(): Unit = {
    val name = "name"
    val array:Array[String] = Array(name)

    def sideEffect(): Unit = {
      // mutate the first element
      array(0) = array(0) + name
    }
    Exit.addShutdownHook(name, sideEffect()) // by-name parameter, not invoked
    // make sure the first element wasn't mutated
    assertEquals(name, array(0))
    Exit.addShutdownHook(name, sideEffect()) // by-name parameter, not invoked
    // again make sure the first element wasn't mutated
    assertEquals(name, array(0))
    Exit.addShutdownHook(name, array(0) = array(0) + name) // by-name parameter, not invoked
    // again make sure the first element wasn't mutated
    assertEquals(name, array(0))
  }
}
