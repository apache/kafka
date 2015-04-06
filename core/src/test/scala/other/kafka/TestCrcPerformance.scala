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
package kafka.log

import java.util.Random
import kafka.message._
import kafka.utils.TestUtils
import org.apache.kafka.common.utils.Utils

object TestCrcPerformance {

  def main(args: Array[String]): Unit = {
    if(args.length < 2)
      Utils.croak("USAGE: java " + getClass().getName() + " num_messages message_size")
    val numMessages = args(0).toInt
    val messageSize = args(1).toInt
    //val numMessages = 100000000
    //val messageSize = 32

    val dir = TestUtils.tempDir()
    val content = new Array[Byte](messageSize)
    new Random(1).nextBytes(content)

    // create message test
    val start = System.nanoTime
    for(i <- 0 until numMessages) {
      new Message(content)
    }
    val ellapsed = System.nanoTime - start
    println("%d messages created in %.2f seconds + (%.2f ns per message).".format(numMessages, ellapsed/(1000.0*1000.0*1000.0),
      ellapsed / numMessages.toDouble))

  }
}
