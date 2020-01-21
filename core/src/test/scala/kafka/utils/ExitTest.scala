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

import org.junit.Assert.assertEquals
import org.junit.Test

class ExitTest {
  @Test
  def shouldHaltImmediately(): Unit = {
    val array:Array[Any] = Array("a", "b")
    def haltProcedure(exitStatus: Int, message: Option[String]) : Nothing = {
      array(0) = exitStatus
      array(1) = message
      throw new Exception()
    }
    Exit.setHaltProcedure(haltProcedure)
    val statusCode = 0
    val message = Some("message")
    try {
      try {
        Exit.halt(statusCode)
      } catch {
        case e: Exception => {
          assertEquals(statusCode, array(0))
          assertEquals(None, array(1))
        }
      }
      try {
        Exit.halt(statusCode, message)
      } catch {
        case e: Exception => {
          assertEquals(statusCode, array(0))
          assertEquals(message, array(1))
        }
      }
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
      throw new Exception()
    }
    Exit.setExitProcedure(exitProcedure)
    val statusCode = 0
    val message = Some("message")
    try {
      try {
        Exit.exit(statusCode)
      } catch {
        case e: Exception => {
          assertEquals(statusCode, array(0))
          assertEquals(None, array(1))
        }
      }
      try {
        Exit.exit(statusCode, message)
      } catch {
        case e: Exception => {
          assertEquals(statusCode, array(0))
          assertEquals(message, array(1))
        }
      }
    } finally {
      Exit.resetExitProcedure()
    }
  }

  @Test
  def shouldAddShutdownHookImmediately(): Unit = {
    val array:Array[Any] = Array(0, Some("other thing"))
    // immediately invoke the code to mutate the data when a hook is added
    def shutdownHookAdder(code: => Unit, name: Option[String]) : Unit = {
      // invoke the code (see below, it mutates the first element)
      code
      // mutate the second element
      array(1) = name
    }
    Exit.setShutdownHookAdder(shutdownHookAdder)
    def sideEffect(): Unit = {
      // mutate the first element
      array(0) = array(0).asInstanceOf[Int] + 1
    }
    val message = Some("message")
    try {
      Exit.addShutdownHook(sideEffect)
      // first element should be mutated once
      assertEquals(1, array(0))
      // second element should be mutated as well
      assertEquals(None, array(1))
      Exit.addShutdownHook(sideEffect(), message)
      // first element should be mutated again, once
      assertEquals(2, array(0))
      // second element should be mutated again, too
      assertEquals(message, array(1))
      Exit.addShutdownHook(array(0) = array(0).asInstanceOf[Int] + 1)
      // first element should be mutated again, once
      assertEquals(3, array(0))
      // second element should be mutated again, too
      assertEquals(None, array(1))
    } finally {
      Exit.resetShutdownHookAdder()
    }
  }

  @Test
  def shouldNotInvokeShutdownHookImmediately(): Unit = {
    val value = "value"
    val array:Array[Any] = Array(value)

    def sideEffect(): Unit = {
      // mutate the first element
      array(0) = array(0).toString + array(0).toString
    }
    Exit.addShutdownHook(sideEffect) // by-name parameter, not invoked
    // make sure the first element wasn't mutated
    assertEquals(value, array(0))
    Exit.addShutdownHook(sideEffect()) // by-name parameter, not invoked
    // again make sure the first element wasn't mutated
    Exit.addShutdownHook(array(0) = array(0).toString + array(0).toString) // by-name parameter, not invoked
    // again make sure the first element wasn't mutated
    assertEquals(value, array(0))
    Exit.addShutdownHook(sideEffect, Some("message")) // by-name parameter, not invoked
    // make sure the first element still isn't mutated
    assertEquals(value, array(0))
  }
}
