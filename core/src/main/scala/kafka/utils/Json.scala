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

import kafka.common._
import util.parsing.json.JSON

/**
 *  A wrapper that synchronizes JSON in scala, which is not threadsafe.
 */
object Json extends Logging {
  val myConversionFunc = {input : String => input.toInt}
  JSON.globalNumberParser = myConversionFunc
  val lock = new Object

  def parseFull(input: String): Option[Any] = {
    lock synchronized {
      try {
        JSON.parseFull(input)
      } catch {
        case t =>
          throw new KafkaException("Can't parse json string: %s".format(input), t)
      }
    }
  }
}