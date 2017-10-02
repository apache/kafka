/**
  * Licensed to the Apache Software Foundation (ASF) under one or more
  * contributor license agreements.  See the NOTICE file distributed with
  * this work for additional information regarding copyright ownership.
  * The ASF licenses this file to You under the Apache License, Version 2.0
  * (the "License"); you may not use this file except in compliance with
  * the License.  You may obtain a copy of the License at
  *
  * http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software
  * distributed under the License is distributed on an "AS IS" BASIS,
  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  * See the License for the specific language governing permissions and
  * limitations under the License.
  */
package kafka.utils

import org.apache.kafka.common.utils.{Exit => JExit}

/**
  * Internal class that should be used instead of `System.exit()` and `Runtime.getRuntime().halt()` so that tests can
  * easily change the behaviour.
  */
object Exit {

  def exit(statusCode: Int, message: Option[String] = None): Nothing = {
    JExit.exit(statusCode, message.orNull)
    throw new AssertionError("exit should not return, but it did.")
  }

  def halt(statusCode: Int, message: Option[String] = None): Nothing = {
    JExit.halt(statusCode, message.orNull)
    throw new AssertionError("halt should not return, but it did.")
  }

  def setExitProcedure(exitProcedure: (Int, Option[String]) => Nothing): Unit =
    JExit.setExitProcedure(functionToProcedure(exitProcedure))

  def setHaltProcedure(haltProcedure: (Int, Option[String]) => Nothing): Unit =
    JExit.setExitProcedure(functionToProcedure(haltProcedure))

  def resetExitProcedure(): Unit =
    JExit.resetExitProcedure()

  def resetHaltProcedure(): Unit =
    JExit.resetHaltProcedure()

  private def functionToProcedure(procedure: (Int, Option[String]) => Nothing) = new JExit.Procedure {
    def execute(statusCode: Int, message: String): Unit = procedure(statusCode, Option(message))
  }

}
