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

package kafka

import kafka.tools.StorageTool
import kafka.utils.Logging

object KafkaNativeWrapper extends Logging {
  def main(args: Array[String]): Unit = {
    if (args.length == 0) {
      throw new RuntimeException(s"Error: No operation input provided. " +
        s"Please provide a valid operation: 'storage-tool' or 'kafka'.")
    }
    val operation = args.head
    val arguments = args.tail
    operation match {
      case "storage-tool" => StorageTool.main(arguments)
      case "kafka" => Kafka.main(arguments)
      case _ =>
        throw new RuntimeException(s"Unknown operation $operation. " +
          s"Please provide a valid operation: 'storage-tool' or 'kafka'.")
    }
  }
}
