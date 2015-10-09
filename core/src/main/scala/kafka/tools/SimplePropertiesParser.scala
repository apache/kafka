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
package kafka.tools

import scala.collection.JavaConverters._
import scala.collection.immutable
import scala.util.control.NonFatal

object SimplePropertiesParser {

  /**
   * Take a simple string of the form key=val,key=val,key=val and parse into a map. Useful for command line
   * arguments.
   * @param freeform String of format "key=val,key=val,key=val"
   * @return
   */
  def propsFromFreeform(freeform: String): java.util.Map[String, String] = {
    val argProps: immutable.Map[String, String] = freeform.split(",")
      .map(_.split("="))
      .map {
        case Array(k, v) => (k, v)
        case _ => exception(freeform)
    }.toMap
    argProps.asJava
  }

  def exception(freeform: String): Nothing = {
    throw new IllegalArgumentException("There was a problem parsing freeform properties string. These should be of the form key=val,key=val... Input was: " + freeform)
  }
}
