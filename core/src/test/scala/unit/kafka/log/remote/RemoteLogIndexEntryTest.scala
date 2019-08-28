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
package kafka.log.remote

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
import java.nio.charset.StandardCharsets

import kafka.utils.TestUtils
import org.junit.Assert.assertEquals
import org.junit.Test

class RemoteLogIndexEntryTest {
  @Test
  def testWriteAndReadInputStream(): Unit = {
    val entries = List(
      RemoteLogIndexEntry(0, 99, 0, 999, 50, "aaa".getBytes(StandardCharsets.UTF_8)),
      RemoteLogIndexEntry(100, 199, 1000, 1999, 50, "bbb".getBytes(StandardCharsets.UTF_8)),
      RemoteLogIndexEntry(200, 299, 2000, 2999, 50, "ccc".getBytes(StandardCharsets.UTF_8))
    )

    val buffers = entries.map(_.asBuffer)

    // It's OK to not close the streams, they're ephemeral.
    val outputStream = new ByteArrayOutputStream(buffers.map(_.limit()).sum)
    buffers.foreach(b => outputStream.write(b.array()))

    val inputStream = new ByteArrayInputStream(outputStream.toByteArray)
    val readEntries = RemoteLogIndexEntry.readAll(inputStream)
    assertEquals(entries, readEntries)
  }

  @Test
  def testWriteAndReadFileChannel(): Unit = {
    val entries = List(
      RemoteLogIndexEntry(0, 99, 0, 999, 50, "aaa".getBytes(StandardCharsets.UTF_8)),
      RemoteLogIndexEntry(100, 199, 1000, 1999, 50, "bbb".getBytes(StandardCharsets.UTF_8)),
      RemoteLogIndexEntry(200, 299, 2000, 2999, 50, "ccc".getBytes(StandardCharsets.UTF_8))
    )

    val channel = TestUtils.tempChannel()
    entries.foreach(e => channel.write(e.asBuffer))
    channel.position(0)

    val readEntries = RemoteLogIndexEntry.readAll(channel)
    assertEquals(entries, readEntries)
  }
}
