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

package kafka

import java.io._
import java.nio._
import java.nio.channels.FileChannel
import java.nio.file.StandardOpenOption

/* This code tests the correct function of java's FileChannel.truncate--some platforms don't work. */
object TestTruncate {

  def main(args: Array[String]): Unit = {
    val name = File.createTempFile("kafka", ".test")
    name.deleteOnExit()
    val file = FileChannel.open(name.toPath, StandardOpenOption.READ, StandardOpenOption.WRITE)
    val buffer = ByteBuffer.allocate(12)
    buffer.putInt(4).putInt(4).putInt(4)
    buffer.rewind()
    file.write(buffer)
    println("position prior to truncate: " + file.position)
    file.truncate(4)
    println("position after truncate to 4: " + file.position)
  }
  
}
