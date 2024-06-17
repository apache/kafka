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
package kafka.log

import kafka.utils.TestUtils
import org.apache.kafka.common.message.FetchResponseData
import org.apache.kafka.storage.internals.log.{AbortedTxn, CorruptIndexException, TransactionIndex}
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.{AfterEach, BeforeEach, Test}

import scala.jdk.CollectionConverters._
import java.io.File
import java.util.Collections

class TransactionIndexTest {
  var file: File = _
  var index: TransactionIndex = _
  val offset = 0L

  @BeforeEach
  def setup(): Unit = {
    file = TestUtils.tempFile()
    index = new TransactionIndex(offset, file)
  }

  @AfterEach
  def teardown(): Unit = {
    index.close()
  }

  @Test
  def testPositionSetCorrectlyWhenOpened(): Unit = {
    val abortedTxns = List(
      new AbortedTxn(0L, 0, 10, 11),
      new AbortedTxn(1L, 5, 15, 13),
      new AbortedTxn(2L, 18, 35, 25),
      new AbortedTxn(3L, 32, 50, 40))
    abortedTxns.foreach(index.append)
    index.close()

    val reopenedIndex = new TransactionIndex(0L, file)
    val anotherAbortedTxn = new AbortedTxn(3L, 50, 60, 55)
    reopenedIndex.append(anotherAbortedTxn)
    assertEquals((abortedTxns ++ List(anotherAbortedTxn)).asJava, reopenedIndex.allAbortedTxns)
  }

  @Test
  def testSanityCheck(): Unit = {
    val abortedTxns = List(
      new AbortedTxn(0L, 0, 10, 11),
      new AbortedTxn(1L, 5, 15, 13),
      new AbortedTxn(2L, 18, 35, 25),
      new AbortedTxn(3L, 32, 50, 40))
    abortedTxns.foreach(index.append)
    index.close()

    // open the index with a different starting offset to fake invalid data
    val reopenedIndex = new TransactionIndex(100L, file)
    assertThrows(classOf[CorruptIndexException], () => reopenedIndex.sanityCheck())
  }

  @Test
  def testLastOffsetMustIncrease(): Unit = {
    index.append(new AbortedTxn(1L, 5, 15, 13))
    assertThrows(classOf[IllegalArgumentException], () => index.append(new AbortedTxn(0L, 0,
      15, 11)))
  }

  @Test
  def testLastOffsetCannotDecrease(): Unit = {
    index.append(new AbortedTxn(1L, 5, 15, 13))
    assertThrows(classOf[IllegalArgumentException], () => index.append(new AbortedTxn(0L, 0,
      10, 11)))
  }

  @Test
  def testCollectAbortedTransactions(): Unit = {
    val abortedTransactions = List(
      new AbortedTxn(0L, 0, 10, 11),
      new AbortedTxn(1L, 5, 15, 13),
      new AbortedTxn(2L, 18, 35, 25),
      new AbortedTxn(3L, 32, 50, 40))

    abortedTransactions.foreach(index.append)

    var result = index.collectAbortedTxns(0L, 100L)
    assertEquals(abortedTransactions.asJava, result.abortedTransactions)
    assertFalse(result.isComplete)

    result = index.collectAbortedTxns(0L, 32)
    assertEquals(abortedTransactions.take(3).asJava, result.abortedTransactions)
    assertTrue(result.isComplete)

    result = index.collectAbortedTxns(0L, 35)
    assertEquals(abortedTransactions.asJava, result.abortedTransactions)
    assertTrue(result.isComplete)

    result = index.collectAbortedTxns(10, 35)
    assertEquals(abortedTransactions.asJava, result.abortedTransactions)
    assertTrue(result.isComplete)

    result = index.collectAbortedTxns(11, 35)
    assertEquals(abortedTransactions.slice(1, 4).asJava, result.abortedTransactions)
    assertTrue(result.isComplete)

    result = index.collectAbortedTxns(20, 41)
    assertEquals(abortedTransactions.slice(2, 4).asJava, result.abortedTransactions)
    assertFalse(result.isComplete)
  }

  @Test
  def testTruncate(): Unit = {
    val abortedTransactions = List(
      new AbortedTxn(0L, 0, 10, 2),
      new AbortedTxn(1L, 5, 15, 16),
      new AbortedTxn(2L, 18, 35, 25),
      new AbortedTxn(3L, 32, 50, 40))

    abortedTransactions.foreach(index.append)

    index.truncateTo(51)
    assertEquals(abortedTransactions.asJava, index.collectAbortedTxns(0L, 100L).abortedTransactions)

    index.truncateTo(50)
    assertEquals(abortedTransactions.take(3).asJava, index.collectAbortedTxns(0L, 100L).abortedTransactions)

    index.reset()
    assertEquals(Collections.emptyList[FetchResponseData.AbortedTransaction], index.collectAbortedTxns(0L, 100L).abortedTransactions)
  }

  @Test
  def testAbortedTxnSerde(): Unit = {
    val pid = 983493L
    val firstOffset = 137L
    val lastOffset = 299L
    val lastStableOffset = 200L

    val abortedTxn = new AbortedTxn(pid, firstOffset, lastOffset, lastStableOffset)
    assertEquals(AbortedTxn.CURRENT_VERSION, abortedTxn.version)
    assertEquals(pid, abortedTxn.producerId)
    assertEquals(firstOffset, abortedTxn.firstOffset)
    assertEquals(lastOffset, abortedTxn.lastOffset)
    assertEquals(lastStableOffset, abortedTxn.lastStableOffset)
  }

  @Test
  def testRenameIndex(): Unit = {
    val renamed = TestUtils.tempFile()
    index.append(new AbortedTxn(0L, 0, 10, 2))

    index.renameTo(renamed)
    index.append(new AbortedTxn(1L, 5, 15, 16))

    val abortedTxns = index.collectAbortedTxns(0L, 100L).abortedTransactions
    assertEquals(2, abortedTxns.size)
    assertEquals(0, abortedTxns.get(0).firstOffset)
    assertEquals(5, abortedTxns.get(1).firstOffset)
  }

  @Test
  def testUpdateParentDir(): Unit = {
    val tmpParentDir = new File(TestUtils.tempDir(), "parent")
    tmpParentDir.mkdir()
    assertNotEquals(tmpParentDir, index.file.getParentFile)
    index.updateParentDir(tmpParentDir)
    assertEquals(tmpParentDir, index.file.getParentFile)
  }
}
