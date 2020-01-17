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
    val message = Some("mesaage")
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
    val message = Some("mesaage")
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
    val array:Array[Any] = Array("a", "b")
    def shutdownHookAdder(runnable: Runnable, name: Option[String]) : Unit = {
      array(0) = runnable
      array(1) = name
    }
    Exit.setShutdownHookAdder(shutdownHookAdder)
    val runnable: Runnable = () => {}
    val message = Some("mesaage")
    try {
      Exit.addShutdownHook(runnable)
      assertEquals(runnable, array(0))
      assertEquals(None, array(1))
      Exit.addShutdownHook(runnable, message)
      assertEquals(runnable, array(0))
      assertEquals(message, array(1))
    } finally {
      Exit.resetShutdownHookAdder()
    }
  }

  @Test
  def shouldNotInvokeShutdownHookImmediately(): Unit = {
    val value = "a"
    val array:Array[Any] = Array(value)
    val runnable: Runnable = () => {}
    try {
        Exit.addShutdownHook(runnable)
        assertEquals(value, array(0))
        Exit.addShutdownHook(runnable, Some("mesaage"))
        assertEquals(value, array(0))
    } finally {
      Exit.resetShutdownHookAdder()
    }
  }
}
