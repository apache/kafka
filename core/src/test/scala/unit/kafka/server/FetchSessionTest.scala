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
package kafka.server

import java.util
import java.util.{Collections, Optional}

import kafka.utils.MockTime
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.record.Records
import org.apache.kafka.common.requests.FetchMetadata.{FINAL_EPOCH, INVALID_SESSION_ID}
import org.apache.kafka.common.requests.{FetchRequest, FetchResponse, FetchMetadata => JFetchMetadata}
import org.junit.Assert._
import org.junit.rules.Timeout
import org.junit.{Rule, Test}

class FetchSessionTest {
  @Rule
  def globalTimeout = Timeout.millis(120000)

  @Test
  def testNewSessionId(): Unit = {
    val cache = new FetchSessionCache(3, 100)
    for (i <- 0 to 10000) {
      val id = cache.newSessionId()
      assertTrue(id > 0)
    }
  }

  def assertCacheContains(cache: FetchSessionCache, sessionIds: Int*) = {
    var i = 0
    for (sessionId <- sessionIds) {
      i = i + 1
      assertTrue("Missing session " + i + " out of " + sessionIds.size + "(" + sessionId + ")",
        cache.get(sessionId).isDefined)
    }
    assertEquals(sessionIds.size, cache.size)
  }

  private def dummyCreate(size: Int)() = {
    val cacheMap = new FetchSession.CACHE_MAP(size)
    for (i <- 0 to (size - 1)) {
      cacheMap.add(new CachedPartition("test", i))
    }
    cacheMap
  }

  @Test
  def testSessionCache(): Unit = {
    val cache = new FetchSessionCache(3, 100)
    assertEquals(0, cache.size)
    val id1 = cache.maybeCreateSession(0, false, 10, () => dummyCreate(10))
    val id2 = cache.maybeCreateSession(10, false, 20, () => dummyCreate(20))
    val id3 = cache.maybeCreateSession(20, false, 30, () => dummyCreate(30))
    assertEquals(INVALID_SESSION_ID, cache.maybeCreateSession(30, false, 40, () => dummyCreate(40)))
    assertEquals(INVALID_SESSION_ID, cache.maybeCreateSession(40, false, 5, () => dummyCreate(5)))
    assertCacheContains(cache, id1, id2, id3)
    cache.touch(cache.get(id1).get, 200)
    val id4 = cache.maybeCreateSession(210, false, 11, () => dummyCreate(11))
    assertCacheContains(cache, id1, id3, id4)
    cache.touch(cache.get(id1).get, 400)
    cache.touch(cache.get(id3).get, 390)
    cache.touch(cache.get(id4).get, 400)
    val id5 = cache.maybeCreateSession(410, false, 50, () => dummyCreate(50))
    assertCacheContains(cache, id3, id4, id5)
    assertEquals(INVALID_SESSION_ID, cache.maybeCreateSession(410, false, 5, () => dummyCreate(5)))
    val id6 = cache.maybeCreateSession(410, true, 5, () => dummyCreate(5))
    assertCacheContains(cache, id3, id5, id6)
  }

  @Test
  def testResizeCachedSessions(): Unit = {
    val cache = new FetchSessionCache(2, 100)
    assertEquals(0, cache.totalPartitions)
    assertEquals(0, cache.size)
    assertEquals(0, cache.evictionsMeter.count)
    val id1 = cache.maybeCreateSession(0, false, 2, () => dummyCreate(2))
    assertTrue(id1 > 0)
    assertCacheContains(cache, id1)
    val session1 = cache.get(id1).get
    assertEquals(2, session1.size)
    assertEquals(2, cache.totalPartitions)
    assertEquals(1, cache.size)
    assertEquals(0, cache.evictionsMeter.count)
    val id2 = cache.maybeCreateSession(0, false, 4, () => dummyCreate(4))
    val session2 = cache.get(id2).get
    assertTrue(id2 > 0)
    assertCacheContains(cache, id1, id2)
    assertEquals(6, cache.totalPartitions)
    assertEquals(2, cache.size)
    assertEquals(0, cache.evictionsMeter.count)
    cache.touch(session1, 200)
    cache.touch(session2, 200)
    val id3 = cache.maybeCreateSession(200, false, 5, () => dummyCreate(5))
    assertTrue(id3 > 0)
    assertCacheContains(cache, id2, id3)
    assertEquals(9, cache.totalPartitions)
    assertEquals(2, cache.size)
    assertEquals(1, cache.evictionsMeter.count)
    cache.remove(id3)
    assertCacheContains(cache, id2)
    assertEquals(1, cache.size)
    assertEquals(1, cache.evictionsMeter.count)
    assertEquals(4, cache.totalPartitions)
    val iter = session2.partitionMap.iterator
    iter.next()
    iter.remove()
    assertEquals(3, session2.size)
    assertEquals(4, session2.cachedSize)
    cache.touch(session2, session2.lastUsedMs)
    assertEquals(3, cache.totalPartitions)
  }

  val EMPTY_PART_LIST = Collections.unmodifiableList(new util.ArrayList[TopicPartition]())

  @Test
  def testFetchRequests(): Unit = {
    val time = new MockTime()
    val cache = new FetchSessionCache(10, 1000)
    val fetchManager = new FetchManager(time, cache)

    // Verify that SESSIONLESS requests get a SessionlessFetchContext
    val context = fetchManager.newContext(JFetchMetadata.LEGACY,
        new util.HashMap[TopicPartition, FetchRequest.PartitionData](), EMPTY_PART_LIST, true)
    assertEquals(classOf[SessionlessFetchContext], context.getClass)

    // Create a new fetch session with a FULL fetch request
    val reqData2 = new util.LinkedHashMap[TopicPartition, FetchRequest.PartitionData]
    reqData2.put(new TopicPartition("foo", 0), new FetchRequest.PartitionData(0, 0, 100,
      Optional.empty()))
    reqData2.put(new TopicPartition("foo", 1), new FetchRequest.PartitionData(10, 0, 100,
      Optional.empty()))
    val context2 = fetchManager.newContext(JFetchMetadata.INITIAL, reqData2, EMPTY_PART_LIST, false)
    assertEquals(classOf[FullFetchContext], context2.getClass)
    val reqData2Iter = reqData2.entrySet().iterator()
    context2.foreachPartition((topicPart, data) => {
      val entry = reqData2Iter.next()
      assertEquals(entry.getKey, topicPart)
      assertEquals(entry.getValue, data)
    })
    assertEquals(0, context2.getFetchOffset(new TopicPartition("foo", 0)).get)
    assertEquals(10, context2.getFetchOffset(new TopicPartition("foo", 1)).get)
    val respData2 = new util.LinkedHashMap[TopicPartition, FetchResponse.PartitionData[Records]]
    respData2.put(new TopicPartition("foo", 0), new FetchResponse.PartitionData(
      Errors.NONE, 100, 100, 100, null, null))
    respData2.put(new TopicPartition("foo", 1), new FetchResponse.PartitionData(
      Errors.NONE, 10, 10, 10, null, null))
    val resp2 = context2.updateAndGenerateResponseData(respData2)
    assertEquals(Errors.NONE, resp2.error())
    assertTrue(resp2.sessionId() != INVALID_SESSION_ID)
    assertEquals(respData2, resp2.responseData())

    // Test trying to create a new session with an invalid epoch
    val context3 = fetchManager.newContext(
      new JFetchMetadata(resp2.sessionId(), 5), reqData2, EMPTY_PART_LIST, false)
    assertEquals(classOf[SessionErrorContext], context3.getClass)
    assertEquals(Errors.INVALID_FETCH_SESSION_EPOCH,
      context3.updateAndGenerateResponseData(respData2).error())

    // Test trying to create a new session with a non-existent session id
    val context4 = fetchManager.newContext(
      new JFetchMetadata(resp2.sessionId() + 1, 1), reqData2, EMPTY_PART_LIST, false)
    assertEquals(classOf[SessionErrorContext], context4.getClass)
    assertEquals(Errors.FETCH_SESSION_ID_NOT_FOUND,
      context4.updateAndGenerateResponseData(respData2).error())

    // Continue the first fetch session we created.
    val reqData5 = new util.LinkedHashMap[TopicPartition, FetchRequest.PartitionData]
    val context5 = fetchManager.newContext(
      new JFetchMetadata(resp2.sessionId(), 1), reqData5, EMPTY_PART_LIST, false)
    assertEquals(classOf[IncrementalFetchContext], context5.getClass)
    val reqData5Iter = reqData2.entrySet().iterator()
    context5.foreachPartition((topicPart, data) => {
      val entry = reqData5Iter.next()
      assertEquals(entry.getKey, topicPart)
      assertEquals(entry.getValue, data)
    })
    assertEquals(10, context5.getFetchOffset(new TopicPartition("foo", 1)).get)
    val resp5 = context5.updateAndGenerateResponseData(respData2)
    assertEquals(Errors.NONE, resp5.error())
    assertEquals(resp2.sessionId(), resp5.sessionId())
    assertEquals(0, resp5.responseData().size())

    // Test setting an invalid fetch session epoch.
    val context6 = fetchManager.newContext(
      new JFetchMetadata(resp2.sessionId(), 5), reqData2, EMPTY_PART_LIST, false)
    assertEquals(classOf[SessionErrorContext], context6.getClass)
    assertEquals(Errors.INVALID_FETCH_SESSION_EPOCH,
      context6.updateAndGenerateResponseData(respData2).error())

    // Test generating a throttled response for the incremental fetch session
    val reqData7 = new util.LinkedHashMap[TopicPartition, FetchRequest.PartitionData]
    val context7 = fetchManager.newContext(
      new JFetchMetadata(resp2.sessionId(), 2), reqData7, EMPTY_PART_LIST, false)
    val resp7 = context7.getThrottledResponse(100)
    assertEquals(Errors.NONE, resp7.error())
    assertEquals(resp2.sessionId(), resp7.sessionId())
    assertEquals(100, resp7.throttleTimeMs())

    // Close the incremental fetch session.
    val prevSessionId = resp5.sessionId
    var nextSessionId = prevSessionId
    do {
      val reqData8 = new util.LinkedHashMap[TopicPartition, FetchRequest.PartitionData]
      reqData8.put(new TopicPartition("bar", 0), new FetchRequest.PartitionData(0, 0, 100,
        Optional.empty()))
      reqData8.put(new TopicPartition("bar", 1), new FetchRequest.PartitionData(10, 0, 100,
        Optional.empty()))
      val context8 = fetchManager.newContext(
        new JFetchMetadata(prevSessionId, FINAL_EPOCH), reqData8, EMPTY_PART_LIST, false)
      assertEquals(classOf[SessionlessFetchContext], context8.getClass)
      assertEquals(0, cache.size)
      val respData8 = new util.LinkedHashMap[TopicPartition, FetchResponse.PartitionData[Records]]
      respData8.put(new TopicPartition("bar", 0),
        new FetchResponse.PartitionData(Errors.NONE, 100, 100, 100, null, null))
      respData8.put(new TopicPartition("bar", 1),
        new FetchResponse.PartitionData(Errors.NONE, 100, 100, 100, null, null))
      val resp8 = context8.updateAndGenerateResponseData(respData8)
      assertEquals(Errors.NONE, resp8.error)
      nextSessionId = resp8.sessionId
    } while (nextSessionId == prevSessionId)
  }

  @Test
  def testIncrementalFetchSession(): Unit = {
    val time = new MockTime()
    val cache = new FetchSessionCache(10, 1000)
    val fetchManager = new FetchManager(time, cache)

    // Create a new fetch session with foo-0 and foo-1
    val reqData1 = new util.LinkedHashMap[TopicPartition, FetchRequest.PartitionData]
    reqData1.put(new TopicPartition("foo", 0), new FetchRequest.PartitionData(0, 0, 100,
      Optional.empty()))
    reqData1.put(new TopicPartition("foo", 1), new FetchRequest.PartitionData(10, 0, 100,
      Optional.empty()))
    val context1 = fetchManager.newContext(JFetchMetadata.INITIAL, reqData1, EMPTY_PART_LIST, false)
    assertEquals(classOf[FullFetchContext], context1.getClass)
    val respData1 = new util.LinkedHashMap[TopicPartition, FetchResponse.PartitionData[Records]]
    respData1.put(new TopicPartition("foo", 0), new FetchResponse.PartitionData(
      Errors.NONE, 100, 100, 100, null, null))
    respData1.put(new TopicPartition("foo", 1), new FetchResponse.PartitionData(
      Errors.NONE, 10, 10, 10, null, null))
    val resp1 = context1.updateAndGenerateResponseData(respData1)
    assertEquals(Errors.NONE, resp1.error())
    assertTrue(resp1.sessionId() != INVALID_SESSION_ID)
    assertEquals(2, resp1.responseData().size())

    // Create an incremental fetch request that removes foo-0 and adds bar-0
    val reqData2 = new util.LinkedHashMap[TopicPartition, FetchRequest.PartitionData]
    reqData2.put(new TopicPartition("bar", 0), new FetchRequest.PartitionData(15, 0, 0,
      Optional.empty()))
    val removed2 = new util.ArrayList[TopicPartition]
    removed2.add(new TopicPartition("foo", 0))
    val context2 = fetchManager.newContext(
      new JFetchMetadata(resp1.sessionId(), 1), reqData2, removed2, false)
    assertEquals(classOf[IncrementalFetchContext], context2.getClass)
    val parts2 = Set(new TopicPartition("foo", 1), new TopicPartition("bar", 0))
    val reqData2Iter = parts2.iterator
    context2.foreachPartition((topicPart, data) => {
      assertEquals(reqData2Iter.next(), topicPart)
    })
    assertEquals(None, context2.getFetchOffset(new TopicPartition("foo", 0)))
    assertEquals(10, context2.getFetchOffset(new TopicPartition("foo", 1)).get)
    assertEquals(15, context2.getFetchOffset(new TopicPartition("bar", 0)).get)
    assertEquals(None, context2.getFetchOffset(new TopicPartition("bar", 2)))
    val respData2 = new util.LinkedHashMap[TopicPartition, FetchResponse.PartitionData[Records]]
    respData2.put(new TopicPartition("foo", 1), new FetchResponse.PartitionData(
      Errors.NONE, 10, 10, 10, null, null))
    respData2.put(new TopicPartition("bar", 0), new FetchResponse.PartitionData(
      Errors.NONE, 10, 10, 10, null, null))
    val resp2 = context2.updateAndGenerateResponseData(respData2)
    assertEquals(Errors.NONE, resp2.error)
    assertEquals(1, resp2.responseData.size)
    assertTrue(resp2.sessionId > 0)
  }

  @Test
  def testZeroSizeFetchSession(): Unit = {
    val time = new MockTime()
    val cache = new FetchSessionCache(10, 1000)
    val fetchManager = new FetchManager(time, cache)

    // Create a new fetch session with foo-0 and foo-1
    val reqData1 = new util.LinkedHashMap[TopicPartition, FetchRequest.PartitionData]
    reqData1.put(new TopicPartition("foo", 0), new FetchRequest.PartitionData(0, 0, 100,
      Optional.empty()))
    reqData1.put(new TopicPartition("foo", 1), new FetchRequest.PartitionData(10, 0, 100,
      Optional.empty()))
    val context1 = fetchManager.newContext(JFetchMetadata.INITIAL, reqData1, EMPTY_PART_LIST, false)
    assertEquals(classOf[FullFetchContext], context1.getClass)
    val respData1 = new util.LinkedHashMap[TopicPartition, FetchResponse.PartitionData[Records]]
    respData1.put(new TopicPartition("foo", 0), new FetchResponse.PartitionData(
      Errors.NONE, 100, 100, 100, null, null))
    respData1.put(new TopicPartition("foo", 1), new FetchResponse.PartitionData(
      Errors.NONE, 10, 10, 10, null, null))
    val resp1 = context1.updateAndGenerateResponseData(respData1)
    assertEquals(Errors.NONE, resp1.error)
    assertTrue(resp1.sessionId() != INVALID_SESSION_ID)
    assertEquals(2, resp1.responseData.size)

    // Create an incremental fetch request that removes foo-0 and foo-1
    // Verify that the previous fetch session was closed.
    val reqData2 = new util.LinkedHashMap[TopicPartition, FetchRequest.PartitionData]
    val removed2 = new util.ArrayList[TopicPartition]
    removed2.add(new TopicPartition("foo", 0))
    removed2.add(new TopicPartition("foo", 1))
    val context2 = fetchManager.newContext(
      new JFetchMetadata(resp1.sessionId, 1), reqData2, removed2, false)
    assertEquals(classOf[SessionlessFetchContext], context2.getClass)
    val respData2 = new util.LinkedHashMap[TopicPartition, FetchResponse.PartitionData[Records]]
    val resp2 = context2.updateAndGenerateResponseData(respData2)
    assertEquals(INVALID_SESSION_ID, resp2.sessionId)
    assertTrue(resp2.responseData().isEmpty)
    assertEquals(0, cache.size)
  }
}
