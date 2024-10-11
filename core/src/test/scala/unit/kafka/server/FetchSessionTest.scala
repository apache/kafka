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

import org.apache.kafka.common.compress.Compression
import org.apache.kafka.common.{TopicIdPartition, TopicPartition, Uuid}
import org.apache.kafka.common.message.FetchResponseData
import org.apache.kafka.common.protocol.{ApiKeys, Errors}
import org.apache.kafka.common.record.MemoryRecords
import org.apache.kafka.common.record.SimpleRecord
import org.apache.kafka.common.requests.FetchMetadata.{FINAL_EPOCH, INVALID_SESSION_ID}
import org.apache.kafka.common.requests.{FetchRequest, FetchResponse, FetchMetadata => JFetchMetadata}
import org.apache.kafka.server.util.MockTime
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.{AfterEach, Test, Timeout}
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.{Arguments, MethodSource, ValueSource}

import scala.jdk.CollectionConverters._
import java.util
import java.util.{Collections, Optional}
import scala.collection.mutable.ArrayBuffer

@Timeout(120)
class FetchSessionTest {

  @AfterEach
  def afterEach(): Unit = {
    FetchSessionCache.metricsGroup.removeMetric(FetchSession.NUM_INCREMENTAL_FETCH_SESSIONS)
    FetchSessionCache.metricsGroup.removeMetric(FetchSession.NUM_INCREMENTAL_FETCH_PARTITIONS_CACHED)
    FetchSessionCache.metricsGroup.removeMetric(FetchSession.INCREMENTAL_FETCH_SESSIONS_EVICTIONS_PER_SEC)
    FetchSessionCache.counter.set(0)
  }

  @Test
  def testNewSessionId(): Unit = {
    val cacheShard = new FetchSessionCacheShard(3, 100)
    for (_ <- 0 to 10000) {
      val id = cacheShard.newSessionId()
      assertTrue(id > 0)
    }
  }

  def assertCacheContains(cacheShard: FetchSessionCacheShard, sessionIds: Int*): Unit = {
    var i = 0
    for (sessionId <- sessionIds) {
      i = i + 1
      assertTrue(cacheShard.get(sessionId).isDefined,
        s"Missing session $i out of ${sessionIds.size} ($sessionId)")
    }
    assertEquals(sessionIds.size, cacheShard.size)
  }

  private def dummyCreate(size: Int): FetchSession.CACHE_MAP = {
    val cacheMap = new FetchSession.CACHE_MAP(size)
    for (i <- 0 until size) {
      cacheMap.add(new CachedPartition("test", Uuid.randomUuid(), i))
    }
    cacheMap
  }

  @Test
  def testSessionCache(): Unit = {
    val cacheShard = new FetchSessionCacheShard(3, 100)
    assertEquals(0, cacheShard.size)
    val id1 = cacheShard.maybeCreateSession(0, privileged = false, 10, usesTopicIds = true, () => dummyCreate(10))
    val id2 = cacheShard.maybeCreateSession(10, privileged = false, 20, usesTopicIds = true, () => dummyCreate(20))
    val id3 = cacheShard.maybeCreateSession(20, privileged = false, 30, usesTopicIds = true, () => dummyCreate(30))
    assertEquals(INVALID_SESSION_ID, cacheShard.maybeCreateSession(30, privileged = false, 40, usesTopicIds = true, () => dummyCreate(40)))
    assertEquals(INVALID_SESSION_ID, cacheShard.maybeCreateSession(40, privileged = false, 5, usesTopicIds = true, () => dummyCreate(5)))
    assertCacheContains(cacheShard, id1, id2, id3)
    cacheShard.touch(cacheShard.get(id1).get, 200)
    val id4 = cacheShard.maybeCreateSession(210, privileged = false, 11, usesTopicIds = true, () => dummyCreate(11))
    assertCacheContains(cacheShard, id1, id3, id4)
    cacheShard.touch(cacheShard.get(id1).get, 400)
    cacheShard.touch(cacheShard.get(id3).get, 390)
    cacheShard.touch(cacheShard.get(id4).get, 400)
    val id5 = cacheShard.maybeCreateSession(410, privileged = false, 50, usesTopicIds = true, () => dummyCreate(50))
    assertCacheContains(cacheShard, id3, id4, id5)
    assertEquals(INVALID_SESSION_ID, cacheShard.maybeCreateSession(410, privileged = false, 5, usesTopicIds = true, () => dummyCreate(5)))
    val id6 = cacheShard.maybeCreateSession(410, privileged = true, 5, usesTopicIds = true, () => dummyCreate(5))
    assertCacheContains(cacheShard, id3, id5, id6)
  }

  @Test
  def testResizeCachedSessions(): Unit = {
    val cacheShard = new FetchSessionCacheShard(2, 100)
    assertEquals(0, cacheShard.totalPartitions)
    assertEquals(0, cacheShard.size)
    assertEquals(0, cacheShard.evictionsMeter.count)
    val id1 = cacheShard.maybeCreateSession(0, privileged = false, 2, usesTopicIds = true, () => dummyCreate(2))
    assertTrue(id1 > 0)
    assertCacheContains(cacheShard, id1)
    val session1 = cacheShard.get(id1).get
    assertEquals(2, session1.size)
    assertEquals(2, cacheShard.totalPartitions)
    assertEquals(1, cacheShard.size)
    assertEquals(0, cacheShard.evictionsMeter.count)
    val id2 = cacheShard.maybeCreateSession(0, privileged = false, 4, usesTopicIds = true, () => dummyCreate(4))
    val session2 = cacheShard.get(id2).get
    assertTrue(id2 > 0)
    assertCacheContains(cacheShard, id1, id2)
    assertEquals(6, cacheShard.totalPartitions)
    assertEquals(2, cacheShard.size)
    assertEquals(0, cacheShard.evictionsMeter.count)
    cacheShard.touch(session1, 200)
    cacheShard.touch(session2, 200)
    val id3 = cacheShard.maybeCreateSession(200, privileged = false, 5, usesTopicIds = true, () => dummyCreate(5))
    assertTrue(id3 > 0)
    assertCacheContains(cacheShard, id2, id3)
    assertEquals(9, cacheShard.totalPartitions)
    assertEquals(2, cacheShard.size)
    assertEquals(1, cacheShard.evictionsMeter.count)
    cacheShard.remove(id3)
    assertCacheContains(cacheShard, id2)
    assertEquals(1, cacheShard.size)
    assertEquals(1, cacheShard.evictionsMeter.count)
    assertEquals(4, cacheShard.totalPartitions)
    val iter = session2.partitionMap.iterator
    iter.next()
    iter.remove()
    assertEquals(3, session2.size)
    assertEquals(4, session2.cachedSize)
    cacheShard.touch(session2, session2.lastUsedMs)
    assertEquals(3, cacheShard.totalPartitions)
  }

  private val EMPTY_PART_LIST = Collections.unmodifiableList(new util.ArrayList[TopicIdPartition]())

  def createRequest(metadata: JFetchMetadata,
                    fetchData: util.Map[TopicPartition, FetchRequest.PartitionData],
                    toForget: util.List[TopicIdPartition], isFromFollower: Boolean,
                    version: Short = ApiKeys.FETCH.latestVersion): FetchRequest = {
    new FetchRequest.Builder(version, version, if (isFromFollower) 1 else FetchRequest.CONSUMER_REPLICA_ID,
      if (isFromFollower) 1 else -1, 0, 0, fetchData).metadata(metadata).removed(toForget).build
  }

  def createRequestWithoutTopicIds(metadata: JFetchMetadata,
                    fetchData: util.Map[TopicPartition, FetchRequest.PartitionData],
                    toForget: util.List[TopicIdPartition], isFromFollower: Boolean): FetchRequest = {
    new FetchRequest.Builder(12, 12, if (isFromFollower) 1 else FetchRequest.CONSUMER_REPLICA_ID,
      if (isFromFollower) 1 else -1, 0, 0, fetchData).metadata(metadata).removed(toForget).build
  }

  @Test
  def testCachedLeaderEpoch(): Unit = {
    val time = new MockTime()
    val cacheShard = new FetchSessionCacheShard(10, 1000)
    val fetchManager = new FetchManager(time, cacheShard)

    val topicIds = Map("foo" -> Uuid.randomUuid(), "bar" -> Uuid.randomUuid()).asJava
    val tp0 = new TopicIdPartition(topicIds.get("foo"), new TopicPartition("foo", 0))
    val tp1 = new TopicIdPartition(topicIds.get("foo"), new TopicPartition("foo", 1))
    val tp2 = new TopicIdPartition(topicIds.get("bar"), new TopicPartition("bar", 1))
    val topicNames = topicIds.asScala.map(_.swap).asJava

    def cachedLeaderEpochs(context: FetchContext): Map[TopicIdPartition, Optional[Integer]] = {
      val mapBuilder = Map.newBuilder[TopicIdPartition, Optional[Integer]]
      context.foreachPartition((tp, data) => mapBuilder += tp -> data.currentLeaderEpoch)
      mapBuilder.result()
    }

    val requestData1 = new util.LinkedHashMap[TopicPartition, FetchRequest.PartitionData]
    requestData1.put(tp0.topicPartition, new FetchRequest.PartitionData(tp0.topicId, 0, 0, 100, Optional.empty()))
    requestData1.put(tp1.topicPartition, new FetchRequest.PartitionData(tp1.topicId, 10, 0, 100, Optional.of(1)))
    requestData1.put(tp2.topicPartition, new FetchRequest.PartitionData(tp2.topicId, 10, 0, 100, Optional.of(2)))

    val request1 = createRequest(JFetchMetadata.INITIAL, requestData1, EMPTY_PART_LIST, isFromFollower = false)
    val context1 = fetchManager.newContext(
      request1.version,
      request1.metadata,
      request1.isFromFollower,
      request1.fetchData(topicNames),
      request1.forgottenTopics(topicNames),
      topicNames
    )
    val epochs1 = cachedLeaderEpochs(context1)
    assertEquals(Optional.empty(), epochs1(tp0))
    assertEquals(Optional.of(1), epochs1(tp1))
    assertEquals(Optional.of(2), epochs1(tp2))

    val response = new util.LinkedHashMap[TopicIdPartition, FetchResponseData.PartitionData]
    response.put(tp0, new FetchResponseData.PartitionData()
      .setPartitionIndex(tp0.partition)
      .setHighWatermark(100)
      .setLastStableOffset(100)
      .setLogStartOffset(100))
    response.put(tp1, new FetchResponseData.PartitionData()
      .setPartitionIndex(tp1.partition)
      .setHighWatermark(10)
      .setLastStableOffset(10)
      .setLogStartOffset(10))
    response.put(tp2, new FetchResponseData.PartitionData()
      .setPartitionIndex(tp2.partition)
      .setHighWatermark(5)
      .setLastStableOffset(5)
      .setLogStartOffset(5))

    val sessionId = context1.updateAndGenerateResponseData(response).sessionId()

    // With no changes, the cached epochs should remain the same
    val requestData2 = new util.LinkedHashMap[TopicPartition, FetchRequest.PartitionData]
    val request2 = createRequest(new JFetchMetadata(sessionId, 1), requestData2, EMPTY_PART_LIST, isFromFollower = false)
    val context2 = fetchManager.newContext(
      request2.version,
      request2.metadata,
      request2.isFromFollower,
      request2.fetchData(topicNames),
      request2.forgottenTopics(topicNames),
      topicNames
    )
    val epochs2 = cachedLeaderEpochs(context2)
    assertEquals(Optional.empty(), epochs1(tp0))
    assertEquals(Optional.of(1), epochs2(tp1))
    assertEquals(Optional.of(2), epochs2(tp2))
    context2.updateAndGenerateResponseData(response).sessionId()

    // Now verify we can change the leader epoch and the context is updated
    val requestData3 = new util.LinkedHashMap[TopicPartition, FetchRequest.PartitionData]
    requestData3.put(tp0.topicPartition, new FetchRequest.PartitionData(tp0.topicId, 0, 0, 100, Optional.of(6)))
    requestData3.put(tp1.topicPartition, new FetchRequest.PartitionData(tp1.topicId, 10, 0, 100, Optional.empty()))
    requestData3.put(tp2.topicPartition, new FetchRequest.PartitionData(tp2.topicId, 10, 0, 100, Optional.of(3)))

    val request3 = createRequest(new JFetchMetadata(sessionId, 2), requestData3, EMPTY_PART_LIST, isFromFollower = false)
    val context3 = fetchManager.newContext(
      request3.version,
      request3.metadata,
      request3.isFromFollower,
      request3.fetchData(topicNames),
      request3.forgottenTopics(topicNames),
      topicNames
    )
    val epochs3 = cachedLeaderEpochs(context3)
    assertEquals(Optional.of(6), epochs3(tp0))
    assertEquals(Optional.empty(), epochs3(tp1))
    assertEquals(Optional.of(3), epochs3(tp2))
  }

  @Test
  def testLastFetchedEpoch(): Unit = {
    val time = new MockTime()
    val cacheShard = new FetchSessionCacheShard(10, 1000)
    val fetchManager = new FetchManager(time, cacheShard)

    val topicIds = Map("foo" -> Uuid.randomUuid(), "bar" -> Uuid.randomUuid()).asJava
    val topicNames = topicIds.asScala.map(_.swap).asJava
    val tp0 = new TopicIdPartition(topicIds.get("foo"), new TopicPartition("foo", 0))
    val tp1 = new TopicIdPartition(topicIds.get("foo"), new TopicPartition("foo", 1))
    val tp2 = new TopicIdPartition(topicIds.get("bar"), new TopicPartition("bar", 1))

    def cachedLeaderEpochs(context: FetchContext): Map[TopicIdPartition, Optional[Integer]] = {
      val mapBuilder = Map.newBuilder[TopicIdPartition, Optional[Integer]]
      context.foreachPartition((tp, data) => mapBuilder += tp -> data.currentLeaderEpoch)
      mapBuilder.result()
    }

    def cachedLastFetchedEpochs(context: FetchContext): Map[TopicIdPartition, Optional[Integer]] = {
      val mapBuilder = Map.newBuilder[TopicIdPartition, Optional[Integer]]
      context.foreachPartition((tp, data) => mapBuilder += tp -> data.lastFetchedEpoch)
      mapBuilder.result()
    }

    val requestData1 = new util.LinkedHashMap[TopicPartition, FetchRequest.PartitionData]
    requestData1.put(tp0.topicPartition, new FetchRequest.PartitionData(tp0.topicId, 0, 0, 100, Optional.empty[Integer], Optional.empty[Integer]))
    requestData1.put(tp1.topicPartition, new FetchRequest.PartitionData(tp1.topicId, 10, 0, 100, Optional.of(1), Optional.empty[Integer]))
    requestData1.put(tp2.topicPartition, new FetchRequest.PartitionData(tp2.topicId, 10, 0, 100, Optional.of(2), Optional.of(1)))

    val request1 = createRequest(JFetchMetadata.INITIAL, requestData1, EMPTY_PART_LIST, isFromFollower = false)
    val context1 = fetchManager.newContext(
      request1.version,
      request1.metadata,
      request1.isFromFollower,
      request1.fetchData(topicNames),
      request1.forgottenTopics(topicNames),
      topicNames
    )
    assertEquals(Map(tp0 -> Optional.empty, tp1 -> Optional.of(1), tp2 -> Optional.of(2)),
      cachedLeaderEpochs(context1))
    assertEquals(Map(tp0 -> Optional.empty, tp1 -> Optional.empty, tp2 -> Optional.of(1)),
      cachedLastFetchedEpochs(context1))

    val response = new util.LinkedHashMap[TopicIdPartition, FetchResponseData.PartitionData]
    response.put(tp0, new FetchResponseData.PartitionData()
      .setPartitionIndex(tp0.partition)
      .setHighWatermark(100)
      .setLastStableOffset(100)
      .setLogStartOffset(100))
    response.put(tp1, new FetchResponseData.PartitionData()
      .setPartitionIndex(tp1.partition)
      .setHighWatermark(10)
      .setLastStableOffset(10)
      .setLogStartOffset(10))
    response.put(tp2, new FetchResponseData.PartitionData()
      .setPartitionIndex(tp2.partition)
      .setHighWatermark(5)
      .setLastStableOffset(5)
      .setLogStartOffset(5))

    val sessionId = context1.updateAndGenerateResponseData(response).sessionId()

    // With no changes, the cached epochs should remain the same
    val requestData2 = new util.LinkedHashMap[TopicPartition, FetchRequest.PartitionData]
    val request2 = createRequest(new JFetchMetadata(sessionId, 1), requestData2, EMPTY_PART_LIST, isFromFollower = false)
    val context2 = fetchManager.newContext(
      request2.version,
      request2.metadata,
      request2.isFromFollower,
      request2.fetchData(topicNames),
      request2.forgottenTopics(topicNames),
      topicNames
    )
    assertEquals(Map(tp0 -> Optional.empty, tp1 -> Optional.of(1), tp2 -> Optional.of(2)), cachedLeaderEpochs(context2))
    assertEquals(Map(tp0 -> Optional.empty, tp1 -> Optional.empty, tp2 -> Optional.of(1)),
      cachedLastFetchedEpochs(context2))
    context2.updateAndGenerateResponseData(response).sessionId()

    // Now verify we can change the leader epoch and the context is updated
    val requestData3 = new util.LinkedHashMap[TopicPartition, FetchRequest.PartitionData]
    requestData3.put(tp0.topicPartition, new FetchRequest.PartitionData(tp0.topicId, 0, 0, 100, Optional.of(6), Optional.of(5)))
    requestData3.put(tp1.topicPartition, new FetchRequest.PartitionData(tp1.topicId, 10, 0, 100, Optional.empty[Integer], Optional.empty[Integer]))
    requestData3.put(tp2.topicPartition, new FetchRequest.PartitionData(tp2.topicId, 10, 0, 100, Optional.of(3), Optional.of(3)))

    val request3 = createRequest(new JFetchMetadata(sessionId, 2), requestData3, EMPTY_PART_LIST, isFromFollower = false)
    val context3 = fetchManager.newContext(
      request3.version,
      request3.metadata,
      request3.isFromFollower,
      request3.fetchData(topicNames),
      request3.forgottenTopics(topicNames),
      topicNames
    )
    assertEquals(Map(tp0 -> Optional.of(6), tp1 -> Optional.empty, tp2 -> Optional.of(3)),
      cachedLeaderEpochs(context3))
    assertEquals(Map(tp0 -> Optional.of(5), tp1 -> Optional.empty, tp2 -> Optional.of(3)),
      cachedLastFetchedEpochs(context2))
  }

  @Test
  def testFetchRequests(): Unit = {
    val time = new MockTime()
    val cacheShard = new FetchSessionCacheShard(10, 1000)
    val fetchManager = new FetchManager(time, cacheShard)
    val topicNames = Map(Uuid.randomUuid() -> "foo", Uuid.randomUuid() -> "bar").asJava
    val topicIds = topicNames.asScala.map(_.swap).asJava
    val tp0 = new TopicIdPartition(topicIds.get("foo"), new TopicPartition("foo", 0))
    val tp1 = new TopicIdPartition(topicIds.get("foo"), new TopicPartition("foo", 1))
    val tp2 = new TopicIdPartition(topicIds.get("bar"), new TopicPartition("bar", 0))
    val tp3 = new TopicIdPartition(topicIds.get("bar"), new TopicPartition("bar", 1))

    // Verify that SESSIONLESS requests get a SessionlessFetchContext
    val request = createRequest(JFetchMetadata.LEGACY, new util.HashMap[TopicPartition, FetchRequest.PartitionData](), EMPTY_PART_LIST, isFromFollower = true)
    val context = fetchManager.newContext(
      request.version,
      request.metadata,
      request.isFromFollower,
      request.fetchData(topicNames),
      request.forgottenTopics(topicNames),
      topicNames
    )
    assertEquals(classOf[SessionlessFetchContext], context.getClass)

    // Create a new fetch session with a FULL fetch request
    val reqData2 = new util.LinkedHashMap[TopicPartition, FetchRequest.PartitionData]
    reqData2.put(tp0.topicPartition, new FetchRequest.PartitionData(tp0.topicId, 0, 0, 100,
      Optional.empty()))
    reqData2.put(tp1.topicPartition, new FetchRequest.PartitionData(tp1.topicId, 10, 0, 100,
      Optional.empty()))
    val request2 = createRequest(JFetchMetadata.INITIAL, reqData2, EMPTY_PART_LIST, isFromFollower = false)
    val context2 = fetchManager.newContext(
      request2.version,
      request2.metadata,
      request2.isFromFollower,
      request2.fetchData(topicNames),
      request2.forgottenTopics(topicNames),
      topicNames
    )
    assertEquals(classOf[FullFetchContext], context2.getClass)
    val reqData2Iter = reqData2.entrySet().iterator()
    context2.foreachPartition((topicIdPart, data) => {
      val entry = reqData2Iter.next()
      assertEquals(entry.getKey, topicIdPart.topicPartition)
      assertEquals(topicIds.get(entry.getKey.topic), topicIdPart.topicId)
      assertEquals(entry.getValue, data)
    })
    assertEquals(0, context2.getFetchOffset(tp0).get)
    assertEquals(10, context2.getFetchOffset(tp1).get)
    val respData2 = new util.LinkedHashMap[TopicIdPartition, FetchResponseData.PartitionData]
    respData2.put(tp0,
      new FetchResponseData.PartitionData()
        .setPartitionIndex(0)
        .setHighWatermark(100)
        .setLastStableOffset(100)
        .setLogStartOffset(100))
    respData2.put(tp1,
      new FetchResponseData.PartitionData()
        .setPartitionIndex(1)
        .setHighWatermark(10)
        .setLastStableOffset(10)
        .setLogStartOffset(10))
    val resp2 = context2.updateAndGenerateResponseData(respData2)
    assertEquals(Errors.NONE, resp2.error())
    assertTrue(resp2.sessionId() != INVALID_SESSION_ID)
    assertEquals(respData2.asScala.map { case (tp, data) => (tp.topicPartition, data)}.toMap.asJava, resp2.responseData(topicNames, request2.version))

    // Test trying to create a new session with an invalid epoch
    val request3 = createRequest(new JFetchMetadata(resp2.sessionId(), 5), reqData2, EMPTY_PART_LIST, isFromFollower = false)
    val context3 = fetchManager.newContext(
      request3.version,
      request3.metadata,
      request3.isFromFollower,
      request3.fetchData(topicNames),
      request3.forgottenTopics(topicNames),
      topicNames
    )
    assertEquals(classOf[SessionErrorContext], context3.getClass)
    assertEquals(Errors.INVALID_FETCH_SESSION_EPOCH,
      context3.updateAndGenerateResponseData(respData2).error())

    // Test trying to create a new session with a non-existent session id
    val request4 = createRequest(new JFetchMetadata(resp2.sessionId() + 1, 1), reqData2, EMPTY_PART_LIST, isFromFollower = false)
    val context4 = fetchManager.newContext(
      request4.version,
      request4.metadata,
      request4.isFromFollower,
      request4.fetchData(topicNames),
      request4.forgottenTopics(topicNames),
      topicNames
    )
    assertEquals(Errors.FETCH_SESSION_ID_NOT_FOUND,
      context4.updateAndGenerateResponseData(respData2).error())

    // Continue the first fetch session we created.
    val reqData5 = new util.LinkedHashMap[TopicPartition, FetchRequest.PartitionData]
    val request5 = createRequest( new JFetchMetadata(resp2.sessionId(), 1), reqData5, EMPTY_PART_LIST, isFromFollower = false)
    val context5 = fetchManager.newContext(
      request5.version,
      request5.metadata,
      request5.isFromFollower,
      request5.fetchData(topicNames),
      request5.forgottenTopics(topicNames),
      topicNames
    )
    assertEquals(classOf[IncrementalFetchContext], context5.getClass)
    val reqData5Iter = reqData2.entrySet().iterator()
    context5.foreachPartition((topicIdPart, data) => {
      val entry = reqData5Iter.next()
      assertEquals(entry.getKey, topicIdPart.topicPartition)
      assertEquals(topicIds.get(entry.getKey.topic()), topicIdPart.topicId)
      assertEquals(entry.getValue, data)
    })
    assertEquals(10, context5.getFetchOffset(tp1).get)
    val resp5 = context5.updateAndGenerateResponseData(respData2)
    assertEquals(Errors.NONE, resp5.error())
    assertEquals(resp2.sessionId(), resp5.sessionId())
    assertEquals(0, resp5.responseData(topicNames, request5.version).size())

    // Test setting an invalid fetch session epoch.
    val request6 = createRequest( new JFetchMetadata(resp2.sessionId(), 5), reqData2, EMPTY_PART_LIST, isFromFollower = false)
    val context6 = fetchManager.newContext(
      request6.version,
      request6.metadata,
      request6.isFromFollower,
      request6.fetchData(topicNames),
      request6.forgottenTopics(topicNames),
      topicNames
    )
    assertEquals(classOf[SessionErrorContext], context6.getClass)
    assertEquals(Errors.INVALID_FETCH_SESSION_EPOCH,
      context6.updateAndGenerateResponseData(respData2).error())

    // Test generating a throttled response for the incremental fetch session
    val reqData7 = new util.LinkedHashMap[TopicPartition, FetchRequest.PartitionData]
    val request7 = createRequest( new JFetchMetadata(resp2.sessionId(), 2), reqData7, EMPTY_PART_LIST, isFromFollower = false)
    val context7 = fetchManager.newContext(
      request7.version,
      request7.metadata,
      request7.isFromFollower,
      request7.fetchData(topicNames),
      request7.forgottenTopics(topicNames),
      topicNames
    )
    val resp7 = context7.getThrottledResponse(100)
    assertEquals(Errors.NONE, resp7.error())
    assertEquals(resp2.sessionId(), resp7.sessionId())
    assertEquals(100, resp7.throttleTimeMs())

    // Close the incremental fetch session.
    val prevSessionId = resp5.sessionId
    var nextSessionId = prevSessionId
    do {
      val reqData8 = new util.LinkedHashMap[TopicPartition, FetchRequest.PartitionData]
      reqData8.put(tp2.topicPartition, new FetchRequest.PartitionData(tp2.topicId, 0, 0, 100,
        Optional.empty()))
      reqData8.put(tp3.topicPartition, new FetchRequest.PartitionData(tp3.topicId, 10, 0, 100,
        Optional.empty()))
      val request8 = createRequest(new JFetchMetadata(prevSessionId, FINAL_EPOCH), reqData8, EMPTY_PART_LIST, isFromFollower = false)
      val context8 = fetchManager.newContext(
        request8.version,
        request8.metadata,
        request8.isFromFollower,
        request8.fetchData(topicNames),
        request8.forgottenTopics(topicNames),
        topicNames
      )
      assertEquals(classOf[SessionlessFetchContext], context8.getClass)
      assertEquals(0, cacheShard.size)
      val respData8 = new util.LinkedHashMap[TopicIdPartition, FetchResponseData.PartitionData]
      respData8.put(tp2,
        new FetchResponseData.PartitionData()
          .setPartitionIndex(0)
          .setHighWatermark(100)
          .setLastStableOffset(100)
          .setLogStartOffset(100))
      respData8.put(tp3,
        new FetchResponseData.PartitionData()
          .setPartitionIndex(1)
          .setHighWatermark(100)
          .setLastStableOffset(100)
          .setLogStartOffset(100))
      val resp8 = context8.updateAndGenerateResponseData(respData8)
      assertEquals(Errors.NONE, resp8.error)
      nextSessionId = resp8.sessionId
    } while (nextSessionId == prevSessionId)
  }

  @ParameterizedTest
  @ValueSource(booleans = Array(true, false))
  def testIncrementalFetchSession(usesTopicIds: Boolean): Unit = {
    val time = new MockTime()
    val cacheShard = new FetchSessionCacheShard(10, 1000)
    val fetchManager = new FetchManager(time, cacheShard)
    val topicNames = if (usesTopicIds) Map(Uuid.randomUuid() -> "foo", Uuid.randomUuid() -> "bar").asJava else Map[Uuid, String]().asJava
    val topicIds = topicNames.asScala.map(_.swap).asJava
    val version = if (usesTopicIds) ApiKeys.FETCH.latestVersion else 12.toShort
    val fooId = topicIds.getOrDefault("foo", Uuid.ZERO_UUID)
    val barId = topicIds.getOrDefault("bar", Uuid.ZERO_UUID)
    val tp0 = new TopicIdPartition(fooId, new TopicPartition("foo", 0))
    val tp1 = new TopicIdPartition(fooId, new TopicPartition("foo", 1))
    val tp2 = new TopicIdPartition(barId, new TopicPartition("bar", 0))

    // Create a new fetch session with foo-0 and foo-1
    val reqData1 = new util.LinkedHashMap[TopicPartition, FetchRequest.PartitionData]
    reqData1.put(tp0.topicPartition, new FetchRequest.PartitionData(fooId,0, 0, 100,
      Optional.empty()))
    reqData1.put(tp1.topicPartition, new FetchRequest.PartitionData(fooId, 10, 0, 100,
      Optional.empty()))
    val request1 = createRequest(JFetchMetadata.INITIAL, reqData1, EMPTY_PART_LIST, isFromFollower = false, version)
    val context1 = fetchManager.newContext(
      request1.version,
      request1.metadata,
      request1.isFromFollower,
      request1.fetchData(topicNames),
      request1.forgottenTopics(topicNames),
      topicNames
    )
    assertEquals(classOf[FullFetchContext], context1.getClass)
    val respData1 = new util.LinkedHashMap[TopicIdPartition, FetchResponseData.PartitionData]
    respData1.put(tp0, new FetchResponseData.PartitionData()
        .setPartitionIndex(0)
        .setHighWatermark(100)
        .setLastStableOffset(100)
        .setLogStartOffset(100))
    respData1.put(tp1, new FetchResponseData.PartitionData()
        .setPartitionIndex(1)
        .setHighWatermark(10)
        .setLastStableOffset(10)
        .setLogStartOffset(10))
    val resp1 = context1.updateAndGenerateResponseData(respData1)
    assertEquals(Errors.NONE, resp1.error())
    assertTrue(resp1.sessionId() != INVALID_SESSION_ID)
    assertEquals(2, resp1.responseData(topicNames, request1.version).size())

    // Create an incremental fetch request that removes foo-0 and adds bar-0
    val reqData2 = new util.LinkedHashMap[TopicPartition, FetchRequest.PartitionData]
    reqData2.put(tp2.topicPartition, new FetchRequest.PartitionData(barId,15, 0, 0,
      Optional.empty()))
    val removed2 = new util.ArrayList[TopicIdPartition]
    removed2.add(tp0)
    val request2 = createRequest(new JFetchMetadata(resp1.sessionId(), 1), reqData2, removed2, isFromFollower = false, version)
    val context2 = fetchManager.newContext(
      request2.version,
      request2.metadata,
      request2.isFromFollower,
      request2.fetchData(topicNames),
      request2.forgottenTopics(topicNames),
      topicNames
    )
    assertEquals(classOf[IncrementalFetchContext], context2.getClass)
    val parts2 = Set(tp1, tp2)
    val reqData2Iter = parts2.iterator
    context2.foreachPartition((topicIdPart, _) => {
      assertEquals(reqData2Iter.next(), topicIdPart)
    })
    assertEquals(None, context2.getFetchOffset(tp0))
    assertEquals(10, context2.getFetchOffset(tp1).get)
    assertEquals(15, context2.getFetchOffset(tp2).get)
    assertEquals(None, context2.getFetchOffset(new TopicIdPartition(barId, new TopicPartition("bar", 2))))
    val respData2 = new util.LinkedHashMap[TopicIdPartition, FetchResponseData.PartitionData]
    respData2.put(tp1, new FetchResponseData.PartitionData()
        .setPartitionIndex(1)
        .setHighWatermark(10)
        .setLastStableOffset(10)
        .setLogStartOffset(10))
    respData2.put(tp2, new FetchResponseData.PartitionData()
        .setPartitionIndex(0)
        .setHighWatermark(10)
        .setLastStableOffset(10)
        .setLogStartOffset(10))
    val resp2 = context2.updateAndGenerateResponseData(respData2)
    assertEquals(Errors.NONE, resp2.error)
    assertEquals(1, resp2.responseData(topicNames, request2.version).size)
    assertTrue(resp2.sessionId > 0)
  }

  // This test simulates a request without IDs sent to a broker with IDs.
  @Test
  def testFetchSessionWithUnknownIdOldRequestVersion(): Unit = {
    val time = new MockTime()
    val cacheShard = new FetchSessionCacheShard(10, 1000)
    val fetchManager = new FetchManager(time, cacheShard)
    val topicNames = Map(Uuid.randomUuid() -> "foo", Uuid.randomUuid() -> "bar").asJava
    val topicIds = topicNames.asScala.map(_.swap).asJava
    val tp0 = new TopicIdPartition(topicIds.get("foo"), new TopicPartition("foo", 0))
    val tp1 = new TopicIdPartition(topicIds.get("foo"), new TopicPartition("foo", 1))

    // Create a new fetch session with foo-0 and foo-1
    val reqData1 = new util.LinkedHashMap[TopicPartition, FetchRequest.PartitionData]
    reqData1.put(tp0.topicPartition, new FetchRequest.PartitionData(tp0.topicId, 0, 0, 100,
      Optional.empty()))
    reqData1.put(tp1.topicPartition, new FetchRequest.PartitionData(Uuid.ZERO_UUID, 10, 0, 100,
      Optional.empty()))
    val request1 = createRequestWithoutTopicIds(JFetchMetadata.INITIAL, reqData1, EMPTY_PART_LIST, isFromFollower = false)
    // Simulate unknown topic ID for foo.
    val topicNamesOnlyBar = Collections.singletonMap(topicIds.get("bar"), "bar")
    // We should not throw error since we have an older request version.
    val context1 = fetchManager.newContext(
      request1.version,
      request1.metadata,
      request1.isFromFollower,
      request1.fetchData(topicNamesOnlyBar),
      request1.forgottenTopics(topicNamesOnlyBar),
      topicNamesOnlyBar
    )
    assertEquals(classOf[FullFetchContext], context1.getClass)
    val respData1 = new util.LinkedHashMap[TopicIdPartition, FetchResponseData.PartitionData]
    respData1.put(tp0, new FetchResponseData.PartitionData()
      .setPartitionIndex(0)
      .setHighWatermark(100)
      .setLastStableOffset(100)
      .setLogStartOffset(100))
    respData1.put(tp1, new FetchResponseData.PartitionData()
      .setPartitionIndex(1)
      .setHighWatermark(10)
      .setLastStableOffset(10)
      .setLogStartOffset(10))
    val resp1 = context1.updateAndGenerateResponseData(respData1)
    // Since we are ignoring IDs, we should have no errors.
    assertEquals(Errors.NONE, resp1.error())
    assertTrue(resp1.sessionId() != INVALID_SESSION_ID)
    assertEquals(2, resp1.responseData(topicNames, request1.version).size)
    resp1.responseData(topicNames, request1.version).forEach( (_, resp) => assertEquals(Errors.NONE.code, resp.errorCode))
  }

  @Test
  def testFetchSessionWithUnknownId(): Unit = {
    val time = new MockTime()
    val cacheShard = new FetchSessionCacheShard(10, 1000)
    val fetchManager = new FetchManager(time, cacheShard)
    val fooId = Uuid.randomUuid()
    val barId = Uuid.randomUuid()
    val zarId = Uuid.randomUuid()
    val topicNames = Map(fooId -> "foo", barId -> "bar", zarId -> "zar").asJava
    val foo0 = new TopicIdPartition(fooId, new TopicPartition("foo", 0))
    val foo1 = new TopicIdPartition(fooId, new TopicPartition("foo", 1))
    val zar0 = new TopicIdPartition(zarId, new TopicPartition("zar", 0))
    val emptyFoo0 = new TopicIdPartition(fooId, new TopicPartition(null, 0))
    val emptyFoo1 = new TopicIdPartition(fooId, new TopicPartition(null, 1))
    val emptyZar0 = new TopicIdPartition(zarId, new TopicPartition(null, 0))

    // Create a new fetch session with foo-0 and foo-1
    val reqData1 = new util.LinkedHashMap[TopicPartition, FetchRequest.PartitionData]
    reqData1.put(foo0.topicPartition, new FetchRequest.PartitionData(foo0.topicId, 0, 0, 100,
      Optional.empty()))
    reqData1.put(foo1.topicPartition, new FetchRequest.PartitionData(foo1.topicId, 10, 0, 100,
      Optional.empty()))
    reqData1.put(zar0.topicPartition, new FetchRequest.PartitionData(zar0.topicId, 10, 0, 100,
      Optional.empty()))
    val request1 = createRequest(JFetchMetadata.INITIAL, reqData1, EMPTY_PART_LIST, isFromFollower = false)
    // Simulate unknown topic ID for foo.
    val topicNamesOnlyBar = Collections.singletonMap(barId, "bar")
    // We should not throw error since we have an older request version.
    val context1 = fetchManager.newContext(
      request1.version,
      request1.metadata,
      request1.isFromFollower,
      request1.fetchData(topicNamesOnlyBar),
      request1.forgottenTopics(topicNamesOnlyBar),
      topicNamesOnlyBar
    )
    assertEquals(classOf[FullFetchContext], context1.getClass)
    assertPartitionsOrder(context1, Seq(emptyFoo0, emptyFoo1, emptyZar0))
    val respData1 = new util.LinkedHashMap[TopicIdPartition, FetchResponseData.PartitionData]
    respData1.put(emptyFoo0, new FetchResponseData.PartitionData()
      .setPartitionIndex(0)
      .setErrorCode(Errors.UNKNOWN_TOPIC_ID.code))
    respData1.put(emptyFoo1, new FetchResponseData.PartitionData()
      .setPartitionIndex(1)
      .setErrorCode(Errors.UNKNOWN_TOPIC_ID.code))
    respData1.put(emptyZar0, new FetchResponseData.PartitionData()
      .setPartitionIndex(1)
      .setErrorCode(Errors.UNKNOWN_TOPIC_ID.code))
    val resp1 = context1.updateAndGenerateResponseData(respData1)
    // On the latest request version, we should have unknown topic ID errors.
    assertEquals(Errors.NONE, resp1.error())
    assertTrue(resp1.sessionId() != INVALID_SESSION_ID)
    assertEquals(
      Map(
        foo0.topicPartition -> Errors.UNKNOWN_TOPIC_ID.code,
        foo1.topicPartition -> Errors.UNKNOWN_TOPIC_ID.code,
        zar0.topicPartition() -> Errors.UNKNOWN_TOPIC_ID.code
      ),
      resp1.responseData(topicNames, request1.version).asScala.map { case (tp, resp) =>
        tp -> resp.errorCode
      }
    )

    // Create an incremental request where we resolve the partitions
    val reqData2 = new util.LinkedHashMap[TopicPartition, FetchRequest.PartitionData]
    val request2 = createRequest(new JFetchMetadata(resp1.sessionId(), 1), reqData2, EMPTY_PART_LIST, isFromFollower = false)
    val topicNamesNoZar = Map(fooId -> "foo", barId -> "bar").asJava
    val context2 = fetchManager.newContext(
      request2.version,
      request2.metadata,
      request2.isFromFollower,
      request2.fetchData(topicNamesNoZar),
      request2.forgottenTopics(topicNamesNoZar),
      topicNamesNoZar
    )
    assertEquals(classOf[IncrementalFetchContext], context2.getClass)
    // Topic names in the session but not in the request are lazily resolved via foreachPartition. Resolve foo topic IDs here.
    assertPartitionsOrder(context2, Seq(foo0, foo1, emptyZar0))
    val respData2 = new util.LinkedHashMap[TopicIdPartition, FetchResponseData.PartitionData]
    respData2.put(foo0, new FetchResponseData.PartitionData()
      .setPartitionIndex(0)
      .setHighWatermark(100)
      .setLastStableOffset(100)
      .setLogStartOffset(100))
    respData2.put(foo1, new FetchResponseData.PartitionData()
      .setPartitionIndex(1)
      .setHighWatermark(10)
      .setLastStableOffset(10)
      .setLogStartOffset(10))
    respData2.put(emptyZar0, new FetchResponseData.PartitionData()
      .setPartitionIndex(1)
      .setErrorCode(Errors.UNKNOWN_TOPIC_ID.code))
    val resp2 = context2.updateAndGenerateResponseData(respData2)
    // Since we are ignoring IDs, we should have no errors.
    assertEquals(Errors.NONE, resp2.error())
    assertTrue(resp2.sessionId() != INVALID_SESSION_ID)
    assertEquals(3, resp2.responseData(topicNames, request2.version).size)
    assertEquals(
      Map(
        foo0.topicPartition -> Errors.NONE.code,
        foo1.topicPartition -> Errors.NONE.code,
        zar0.topicPartition -> Errors.UNKNOWN_TOPIC_ID.code
      ),
      resp2.responseData(topicNames, request2.version).asScala.map { case (tp, resp) =>
        tp -> resp.errorCode
      }
    )
  }

  @Test
  def testIncrementalFetchSessionWithIdsWhenSessionDoesNotUseIds() : Unit = {
    val time = new MockTime()
    val cacheShard = new FetchSessionCacheShard(10, 1000)
    val fetchManager = new FetchManager(time, cacheShard)
    val topicNames = new util.HashMap[Uuid, String]()
    val foo0 = new TopicIdPartition(Uuid.ZERO_UUID, new TopicPartition("foo", 0))

    // Create a new fetch session with foo-0
    val reqData1 = new util.LinkedHashMap[TopicPartition, FetchRequest.PartitionData]
    reqData1.put(foo0.topicPartition, new FetchRequest.PartitionData(Uuid.ZERO_UUID, 0, 0, 100,
      Optional.empty()))
    val request1 = createRequestWithoutTopicIds(JFetchMetadata.INITIAL, reqData1, EMPTY_PART_LIST, isFromFollower = false)
    // Start a fetch session using a request version that does not use topic IDs.
    val context1 = fetchManager.newContext(
      request1.version,
      request1.metadata,
      request1.isFromFollower,
      request1.fetchData(topicNames),
      request1.forgottenTopics(topicNames),
      topicNames
    )
    assertEquals(classOf[FullFetchContext], context1.getClass)
    val respData1 = new util.LinkedHashMap[TopicIdPartition, FetchResponseData.PartitionData]
    respData1.put(foo0, new FetchResponseData.PartitionData()
      .setPartitionIndex(0)
      .setHighWatermark(100)
      .setLastStableOffset(100)
      .setLogStartOffset(100))
    val resp1 = context1.updateAndGenerateResponseData(respData1)
    assertEquals(Errors.NONE, resp1.error())
    assertTrue(resp1.sessionId() != INVALID_SESSION_ID)

    // Create an incremental fetch request as though no topics changed. However, send a v13 request.
    // Also simulate the topic ID found on the server.
    val fooId = Uuid.randomUuid()
    topicNames.put(fooId, "foo")
    val reqData2 = new util.LinkedHashMap[TopicPartition, FetchRequest.PartitionData]
    val request2 = createRequest(new JFetchMetadata(resp1.sessionId(), 1), reqData2, EMPTY_PART_LIST, isFromFollower = false)
    val context2 = fetchManager.newContext(
      request2.version,
      request2.metadata,
      request2.isFromFollower,
      request2.fetchData(topicNames),
      request2.forgottenTopics(topicNames),
      topicNames
    )

    assertEquals(classOf[SessionErrorContext], context2.getClass)
    val respData2 = new util.LinkedHashMap[TopicIdPartition, FetchResponseData.PartitionData]
    assertEquals(Errors.FETCH_SESSION_TOPIC_ID_ERROR,
      context2.updateAndGenerateResponseData(respData2).error())
  }

  @Test
  def testIncrementalFetchSessionWithoutIdsWhenSessionUsesIds() : Unit = {
    val time = new MockTime()
    val cacheShard = new FetchSessionCacheShard(10, 1000)
    val fetchManager = new FetchManager(time, cacheShard)
    val fooId = Uuid.randomUuid()
    val topicNames = new util.HashMap[Uuid, String]()
    topicNames.put(fooId, "foo")
    val foo0 = new TopicIdPartition(fooId, new TopicPartition("foo", 0))

    // Create a new fetch session with foo-0
    val reqData1 = new util.LinkedHashMap[TopicPartition, FetchRequest.PartitionData]
    reqData1.put(foo0.topicPartition, new FetchRequest.PartitionData(fooId,0, 0, 100,
      Optional.empty()))
    val request1 = createRequest(JFetchMetadata.INITIAL, reqData1, EMPTY_PART_LIST, isFromFollower = false)
    // Start a fetch session using a request version that uses topic IDs.
    val context1 = fetchManager.newContext(
      request1.version,
      request1.metadata,
      request1.isFromFollower,
      request1.fetchData(topicNames),
      request1.forgottenTopics(topicNames),
      topicNames
    )
    assertEquals(classOf[FullFetchContext], context1.getClass)
    val respData1 = new util.LinkedHashMap[TopicIdPartition, FetchResponseData.PartitionData]
    respData1.put(foo0, new FetchResponseData.PartitionData()
      .setPartitionIndex(0)
      .setHighWatermark(100)
      .setLastStableOffset(100)
      .setLogStartOffset(100))
    val resp1 = context1.updateAndGenerateResponseData(respData1)
    assertEquals(Errors.NONE, resp1.error())
    assertTrue(resp1.sessionId() != INVALID_SESSION_ID)

    // Create an incremental fetch request as though no topics changed. However, send a v12 request.
    // Also simulate the topic ID not found on the server
    topicNames.remove(fooId)
    val reqData2 = new util.LinkedHashMap[TopicPartition, FetchRequest.PartitionData]
    val request2 = createRequestWithoutTopicIds(new JFetchMetadata(resp1.sessionId(), 1), reqData2, EMPTY_PART_LIST, isFromFollower = false)
    val context2 = fetchManager.newContext(
      request2.version,
      request2.metadata,
      request2.isFromFollower,
      request2.fetchData(topicNames),
      request2.forgottenTopics(topicNames),
      topicNames
    )

    assertEquals(classOf[SessionErrorContext], context2.getClass)
    val respData2 = new util.LinkedHashMap[TopicIdPartition, FetchResponseData.PartitionData]
    assertEquals(Errors.FETCH_SESSION_TOPIC_ID_ERROR,
      context2.updateAndGenerateResponseData(respData2).error())
  }

  // This test simulates a session where the topic ID changes broker side (the one handling the request) in both the metadata cache and the log
  // -- as though the topic is deleted and recreated.
  @Test
  def testFetchSessionUpdateTopicIdsBrokerSide(): Unit = {
    val time = new MockTime()
    val cacheShard = new FetchSessionCacheShard(10, 1000)
    val fetchManager = new FetchManager(time, cacheShard)
    val topicNames = Map(Uuid.randomUuid() -> "foo", Uuid.randomUuid() -> "bar").asJava
    val topicIds = topicNames.asScala.map(_.swap).asJava
    val tp0 = new TopicIdPartition(topicIds.get("foo"), new TopicPartition("foo", 0))
    val tp1 = new TopicIdPartition(topicIds.get("bar"), new TopicPartition("bar", 1))

    // Create a new fetch session with foo-0 and bar-1
    val reqData1 = new util.LinkedHashMap[TopicPartition, FetchRequest.PartitionData]
    reqData1.put(tp0.topicPartition, new FetchRequest.PartitionData(tp0.topicId, 0, 0, 100,
      Optional.empty()))
    reqData1.put(tp1.topicPartition, new FetchRequest.PartitionData(tp1.topicId, 10, 0, 100,
      Optional.empty()))
    val request1 = createRequest(JFetchMetadata.INITIAL, reqData1, EMPTY_PART_LIST, isFromFollower = false)
    // Start a fetch session. Simulate unknown partition foo-0.
    val context1 = fetchManager.newContext(
      request1.version,
      request1.metadata,
      request1.isFromFollower,
      request1.fetchData(topicNames),
      request1.forgottenTopics(topicNames),
      topicNames
    )
    assertEquals(classOf[FullFetchContext], context1.getClass)
    val respData1 = new util.LinkedHashMap[TopicIdPartition, FetchResponseData.PartitionData]
    respData1.put(tp1, new FetchResponseData.PartitionData()
      .setPartitionIndex(1)
      .setHighWatermark(10)
      .setLastStableOffset(10)
      .setLogStartOffset(10))
    respData1.put(tp0, new FetchResponseData.PartitionData()
      .setPartitionIndex(0)
      .setHighWatermark(-1)
      .setLastStableOffset(-1)
      .setLogStartOffset(-1)
      .setErrorCode(Errors.UNKNOWN_TOPIC_OR_PARTITION.code))
    val resp1 = context1.updateAndGenerateResponseData(respData1)
    assertEquals(Errors.NONE, resp1.error())
    assertTrue(resp1.sessionId() != INVALID_SESSION_ID)
    assertEquals(2, resp1.responseData(topicNames, request1.version).size)

    // Create an incremental fetch request as though no topics changed.
    val reqData2 = new util.LinkedHashMap[TopicPartition, FetchRequest.PartitionData]
    val request2 = createRequest(new JFetchMetadata(resp1.sessionId(), 1), reqData2, EMPTY_PART_LIST, isFromFollower = false)
    // Simulate ID changing on server.
    val topicNamesFooChanged =  Map(topicIds.get("bar") -> "bar", Uuid.randomUuid() -> "foo").asJava
    val context2 = fetchManager.newContext(
      request2.version,
      request2.metadata,
      request2.isFromFollower,
      request2.fetchData(topicNamesFooChanged),
      request2.forgottenTopics(topicNamesFooChanged),
      topicNamesFooChanged
    )
    assertEquals(classOf[IncrementalFetchContext], context2.getClass)
    val respData2 = new util.LinkedHashMap[TopicIdPartition, FetchResponseData.PartitionData]
    // Likely if the topic ID is different in the broker, it will be different in the log. Simulate the log check finding an inconsistent ID.
    respData2.put(tp0, new FetchResponseData.PartitionData()
      .setPartitionIndex(0)
      .setHighWatermark(-1)
      .setLastStableOffset(-1)
      .setLogStartOffset(-1)
      .setErrorCode(Errors.INCONSISTENT_TOPIC_ID.code))
    val resp2 = context2.updateAndGenerateResponseData(respData2)

    assertEquals(Errors.NONE, resp2.error)
    assertTrue(resp2.sessionId > 0)
    val responseData2 = resp2.responseData(topicNames, request2.version)
    // We should have the inconsistent topic ID error on the partition
    assertEquals(Errors.INCONSISTENT_TOPIC_ID.code, responseData2.get(tp0.topicPartition).errorCode)
  }

  private def noErrorResponse: FetchResponseData.PartitionData = {
    new FetchResponseData.PartitionData()
      .setPartitionIndex(1)
      .setHighWatermark(10)
      .setLastStableOffset(10)
      .setLogStartOffset(10)
  }

  private def errorResponse(errorCode: Short): FetchResponseData.PartitionData  = {
    new FetchResponseData.PartitionData()
      .setPartitionIndex(0)
      .setHighWatermark(-1)
      .setLastStableOffset(-1)
      .setLogStartOffset(-1)
      .setErrorCode(errorCode)
  }

  @Test
  def testResolveUnknownPartitions(): Unit = {
    val time = new MockTime()
    val cacheShard = new FetchSessionCacheShard(10, 1000)
    val fetchManager = new FetchManager(time, cacheShard)

    def newContext(
      metadata: JFetchMetadata,
      partitions: Seq[TopicIdPartition],
      topicNames: Map[Uuid, String] // Topic ID to name mapping known by the broker.
    ): FetchContext = {
      val data = new util.LinkedHashMap[TopicPartition, FetchRequest.PartitionData]
      partitions.foreach { topicIdPartition =>
        data.put(
          topicIdPartition.topicPartition,
          new FetchRequest.PartitionData(topicIdPartition.topicId, 0, 0, 100, Optional.empty())
        )
      }

      val fetchRequest = createRequest(metadata, data, EMPTY_PART_LIST, isFromFollower = false)

      fetchManager.newContext(
        fetchRequest.version,
        fetchRequest.metadata,
        fetchRequest.isFromFollower,
        fetchRequest.fetchData(topicNames.asJava),
        fetchRequest.forgottenTopics(topicNames.asJava),
        topicNames.asJava
      )
    }

    def updateAndGenerateResponseData(
      context: FetchContext
    ): Int = {
      val data = new util.LinkedHashMap[TopicIdPartition, FetchResponseData.PartitionData]
      context.foreachPartition { (topicIdPartition, _) =>
        data.put(
          topicIdPartition,
          if (topicIdPartition.topic == null)
            errorResponse(Errors.UNKNOWN_TOPIC_ID.code)
          else
            noErrorResponse
        )
      }
      context.updateAndGenerateResponseData(data).sessionId
    }

    val foo = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("foo", 0))
    val bar = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("bar", 0))
    val zar = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("zar", 0))

    val fooUnresolved = new TopicIdPartition(foo.topicId, new TopicPartition(null, foo.partition))
    val barUnresolved = new TopicIdPartition(bar.topicId, new TopicPartition(null, bar.partition))
    val zarUnresolved = new TopicIdPartition(zar.topicId, new TopicPartition(null, zar.partition))

    // The metadata cache does not know about the topic.
    val context1 = newContext(
      JFetchMetadata.INITIAL,
      Seq(foo, bar, zar),
      Map.empty[Uuid, String]
    )

    // So the context contains unresolved partitions.
    assertEquals(classOf[FullFetchContext], context1.getClass)
    assertPartitionsOrder(context1, Seq(fooUnresolved, barUnresolved, zarUnresolved))

    // The response is sent back to create the session.
    val sessionId = updateAndGenerateResponseData(context1)

    // The metadata cache only knows about foo.
    val context2 = newContext(
      new JFetchMetadata(sessionId, 1),
      Seq.empty,
      Map(foo.topicId -> foo.topic)
    )

    // So foo is resolved but not the others.
    assertEquals(classOf[IncrementalFetchContext], context2.getClass)
    assertPartitionsOrder(context2, Seq(foo, barUnresolved, zarUnresolved))

    updateAndGenerateResponseData(context2)

    // The metadata cache knows about foo and bar.
    val context3 = newContext(
      new JFetchMetadata(sessionId, 2),
      Seq(bar),
      Map(foo.topicId -> foo.topic, bar.topicId -> bar.topic)
    )

    // So foo and bar are resolved.
    assertEquals(classOf[IncrementalFetchContext], context3.getClass)
    assertPartitionsOrder(context3, Seq(foo, bar, zarUnresolved))

    updateAndGenerateResponseData(context3)

    // The metadata cache knows about all topics.
    val context4 = newContext(
      new JFetchMetadata(sessionId, 3),
      Seq.empty,
      Map(foo.topicId -> foo.topic, bar.topicId -> bar.topic, zar.topicId -> zar.topic)
    )

    // So all topics are resolved.
    assertEquals(classOf[IncrementalFetchContext], context4.getClass)
    assertPartitionsOrder(context4, Seq(foo, bar, zar))

    updateAndGenerateResponseData(context4)

    // The metadata cache does not know about the topics anymore (e.g. deleted).
    val context5 = newContext(
      new JFetchMetadata(sessionId, 4),
      Seq.empty,
      Map.empty
    )

    // All topics remain resolved.
    assertEquals(classOf[IncrementalFetchContext], context5.getClass)
    assertPartitionsOrder(context4, Seq(foo, bar, zar))
  }

  // This test simulates trying to forget a topic partition with all possible topic ID usages for both requests.
  @ParameterizedTest
  @MethodSource(Array("idUsageCombinations"))
  def testToForgetPartitions(fooStartsResolved: Boolean, fooEndsResolved: Boolean): Unit = {
    val time = new MockTime()
    val cacheShard = new FetchSessionCacheShard(10, 1000)
    val fetchManager = new FetchManager(time, cacheShard)

    def newContext(
      metadata: JFetchMetadata,
      partitions: Seq[TopicIdPartition],
      toForget: Seq[TopicIdPartition],
      topicNames: Map[Uuid, String] // Topic ID to name mapping known by the broker.
    ): FetchContext = {
      val data = new util.LinkedHashMap[TopicPartition, FetchRequest.PartitionData]
      partitions.foreach { topicIdPartition =>
        data.put(
          topicIdPartition.topicPartition,
          new FetchRequest.PartitionData(topicIdPartition.topicId, 0, 0, 100, Optional.empty())
        )
      }

      val fetchRequest = createRequest(metadata, data, toForget.toList.asJava, isFromFollower = false)

      fetchManager.newContext(
        fetchRequest.version,
        fetchRequest.metadata,
        fetchRequest.isFromFollower,
        fetchRequest.fetchData(topicNames.asJava),
        fetchRequest.forgottenTopics(topicNames.asJava),
        topicNames.asJava
      )
    }

    def updateAndGenerateResponseData(
      context: FetchContext
    ): Int = {
      val data = new util.LinkedHashMap[TopicIdPartition, FetchResponseData.PartitionData]
      context.foreachPartition { (topicIdPartition, _) =>
        data.put(
          topicIdPartition,
          if (topicIdPartition.topic == null)
            errorResponse(Errors.UNKNOWN_TOPIC_ID.code)
          else
            noErrorResponse
        )
      }
      context.updateAndGenerateResponseData(data).sessionId
    }

    val foo = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("foo", 0))
    val bar = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("bar", 0))

    val fooUnresolved = new TopicIdPartition(foo.topicId, new TopicPartition(null, foo.partition))
    val barUnresolved = new TopicIdPartition(bar.topicId, new TopicPartition(null, bar.partition))

    // Create a new context where foo's resolution depends on fooStartsResolved and bar is unresolved.
    val context1Names = if (fooStartsResolved) Map(foo.topicId -> foo.topic) else Map.empty[Uuid, String]
    val fooContext1 = if (fooStartsResolved) foo else fooUnresolved
    val context1 = newContext(
      JFetchMetadata.INITIAL,
      Seq(fooContext1, bar),
      Seq.empty,
      context1Names
    )

    // So the context contains unresolved bar and a resolved foo iff fooStartsResolved
    assertEquals(classOf[FullFetchContext], context1.getClass)
    assertPartitionsOrder(context1, Seq(fooContext1, barUnresolved))

    // The response is sent back to create the session.
    val sessionId = updateAndGenerateResponseData(context1)

    // Forget foo, but keep bar. Foo's resolution depends on fooEndsResolved and bar stays unresolved.
    val context2Names = if (fooEndsResolved) Map(foo.topicId -> foo.topic) else Map.empty[Uuid, String]
    val fooContext2 = if (fooEndsResolved) foo else fooUnresolved
    val context2 = newContext(
      new JFetchMetadata(sessionId, 1),
      Seq.empty,
      Seq(fooContext2),
      context2Names
    )

    // So foo is removed but not the others.
    assertEquals(classOf[IncrementalFetchContext], context2.getClass)
    assertPartitionsOrder(context2, Seq(barUnresolved))

    updateAndGenerateResponseData(context2)

    // Now remove bar
    val context3 = newContext(
      new JFetchMetadata(sessionId, 2),
      Seq.empty,
      Seq(bar),
      Map.empty[Uuid, String]
    )

    // Context is sessionless since it is empty.
    assertEquals(classOf[SessionlessFetchContext], context3.getClass)
    assertPartitionsOrder(context3, Seq())
  }

  @Test
  def testUpdateAndGenerateResponseData(): Unit = {
    val time = new MockTime()
    val cacheShard = new FetchSessionCacheShard(10, 1000)
    val fetchManager = new FetchManager(time, cacheShard)

    def newContext(
      metadata: JFetchMetadata,
      partitions: Seq[TopicIdPartition],
      topicNames: Map[Uuid, String] // Topic ID to name mapping known by the broker.
    ): FetchContext = {
      val data = new util.LinkedHashMap[TopicPartition, FetchRequest.PartitionData]
      partitions.foreach { topicIdPartition =>
        data.put(
          topicIdPartition.topicPartition,
          new FetchRequest.PartitionData(topicIdPartition.topicId, 0, 0, 100, Optional.empty())
        )
      }

      val fetchRequest = createRequest(metadata, data, EMPTY_PART_LIST, isFromFollower = false)

      fetchManager.newContext(
        fetchRequest.version,
        fetchRequest.metadata,
        fetchRequest.isFromFollower,
        fetchRequest.fetchData(topicNames.asJava),
        fetchRequest.forgottenTopics(topicNames.asJava),
        topicNames.asJava
      )
    }

    // Give both topics errors so they will stay in the session.
    def updateAndGenerateResponseData(
      context: FetchContext
    ): FetchResponse = {
      val data = new util.LinkedHashMap[TopicIdPartition, FetchResponseData.PartitionData]
      context.foreachPartition { (topicIdPartition, _) =>
        data.put(
          topicIdPartition,
          if (topicIdPartition.topic == null)
            errorResponse(Errors.UNKNOWN_TOPIC_ID.code)
          else
            errorResponse(Errors.UNKNOWN_TOPIC_OR_PARTITION.code)
        )
      }
      context.updateAndGenerateResponseData(data)
    }

    val foo = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("foo", 0))
    val bar = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("bar", 0))

    // Foo will always be resolved and bar will always not be resolved on the receiving broker.
    val receivingBrokerTopicNames = Map(foo.topicId -> foo.topic)
    // The sender will know both topics' id to name mappings.
    val sendingTopicNames = Map(foo.topicId -> foo.topic, bar.topicId -> bar.topic)

    def checkResponseData(response: FetchResponse): Unit = {
      assertEquals(
        Map(
          foo.topicPartition -> Errors.UNKNOWN_TOPIC_OR_PARTITION.code,
          bar.topicPartition -> Errors.UNKNOWN_TOPIC_ID.code,
        ),
        response.responseData(sendingTopicNames.asJava, ApiKeys.FETCH.latestVersion).asScala.map { case (tp, resp) =>
          tp -> resp.errorCode
        }
      )
    }

    // Start with a sessionless context.
    val context1 = newContext(
      JFetchMetadata.LEGACY,
      Seq(foo, bar),
      receivingBrokerTopicNames
    )
    assertEquals(classOf[SessionlessFetchContext], context1.getClass)
    // Check the response can be read as expected.
    checkResponseData(updateAndGenerateResponseData(context1))

    // Now create a full context.
    val context2 = newContext(
      JFetchMetadata.INITIAL,
      Seq(foo, bar),
      receivingBrokerTopicNames
    )
    assertEquals(classOf[FullFetchContext], context2.getClass)
    // We want to get the session ID to build more contexts in this session.
    val response2 = updateAndGenerateResponseData(context2)
    val sessionId = response2.sessionId
    checkResponseData(response2)

    // Now create an incremental context. We re-add foo as though the partition data is updated. In a real broker, the data would update.
    val context3 = newContext(
      new JFetchMetadata(sessionId, 1),
      Seq.empty,
      receivingBrokerTopicNames
    )
    assertEquals(classOf[IncrementalFetchContext], context3.getClass)
    checkResponseData(updateAndGenerateResponseData(context3))

    // Finally create an error context by using the same epoch
    val context4 = newContext(
      new JFetchMetadata(sessionId, 1),
      Seq.empty,
      receivingBrokerTopicNames
    )
    assertEquals(classOf[SessionErrorContext], context4.getClass)
    // The response should be empty.
    assertEquals(Collections.emptyList, updateAndGenerateResponseData(context4).data.responses)
  }

  @Test
  def testFetchSessionExpiration(): Unit = {
    val time = new MockTime()
    // set maximum entries to 2 to allow for eviction later
    val cacheShard = new FetchSessionCacheShard(2, 1000)
    val fetchManager = new FetchManager(time, cacheShard)
    val fooId = Uuid.randomUuid()
    val topicNames = Map(fooId -> "foo").asJava
    val foo0 = new TopicIdPartition(fooId, new TopicPartition("foo", 0))
    val foo1 = new TopicIdPartition(fooId, new TopicPartition("foo", 1))

    // Create a new fetch session, session 1
    val session1req = new util.LinkedHashMap[TopicPartition, FetchRequest.PartitionData]
    session1req.put(foo0.topicPartition, new FetchRequest.PartitionData(fooId,0, 0, 100,
      Optional.empty()))
    session1req.put(foo1.topicPartition, new FetchRequest.PartitionData(fooId,10, 0, 100,
      Optional.empty()))
    val session1request1 = createRequest(JFetchMetadata.INITIAL, session1req, EMPTY_PART_LIST, isFromFollower = false)
    val session1context1 = fetchManager.newContext(
      session1request1.version,
      session1request1.metadata,
      session1request1.isFromFollower,
      session1request1.fetchData(topicNames),
      session1request1.forgottenTopics(topicNames),
      topicNames
    )
    assertEquals(classOf[FullFetchContext], session1context1.getClass)
    val respData1 = new util.LinkedHashMap[TopicIdPartition, FetchResponseData.PartitionData]
    respData1.put(foo0, new FetchResponseData.PartitionData()
        .setPartitionIndex(0)
        .setHighWatermark(100)
        .setLastStableOffset(100)
        .setLogStartOffset(100))
    respData1.put(foo1, new FetchResponseData.PartitionData()
        .setPartitionIndex(1)
        .setHighWatermark(10)
        .setLastStableOffset(10)
        .setLogStartOffset(10))
    val session1resp = session1context1.updateAndGenerateResponseData(respData1)
    assertEquals(Errors.NONE, session1resp.error())
    assertTrue(session1resp.sessionId() != INVALID_SESSION_ID)
    assertEquals(2, session1resp.responseData(topicNames, session1request1.version).size)

    // check session entered into case
    assertTrue(cacheShard.get(session1resp.sessionId()).isDefined)
    time.sleep(500)

    // Create a second new fetch session
    val session2req = new util.LinkedHashMap[TopicPartition, FetchRequest.PartitionData]
    session2req.put(foo0.topicPartition, new FetchRequest.PartitionData(fooId, 0, 0, 100,
      Optional.empty()))
    session2req.put(foo1.topicPartition, new FetchRequest.PartitionData(fooId,10, 0, 100,
      Optional.empty()))
    val session2request1 = createRequest(JFetchMetadata.INITIAL, session1req, EMPTY_PART_LIST, isFromFollower = false)
    val session2context = fetchManager.newContext(
      session2request1.version,
      session2request1.metadata,
      session2request1.isFromFollower,
      session2request1.fetchData(topicNames),
      session2request1.forgottenTopics(topicNames),
      topicNames
    )
    assertEquals(classOf[FullFetchContext], session2context.getClass)
    val session2RespData = new util.LinkedHashMap[TopicPartition, FetchResponseData.PartitionData]
    session2RespData.put(foo0.topicPartition,
      new FetchResponseData.PartitionData()
        .setPartitionIndex(0)
        .setHighWatermark(100)
        .setLastStableOffset(100)
        .setLogStartOffset(100))
    session2RespData.put(foo1.topicPartition, new FetchResponseData.PartitionData()
      .setPartitionIndex(1)
      .setHighWatermark(10)
      .setLastStableOffset(10)
      .setLogStartOffset(10))
    val session2resp = session2context.updateAndGenerateResponseData(respData1)
    assertEquals(Errors.NONE, session2resp.error())
    assertTrue(session2resp.sessionId() != INVALID_SESSION_ID)
    assertEquals(2, session2resp.responseData(topicNames, session2request1.version()).size())

    // both newly created entries are present in cache
    assertTrue(cacheShard.get(session1resp.sessionId()).isDefined)
    assertTrue(cacheShard.get(session2resp.sessionId()).isDefined)
    time.sleep(500)

    // Create an incremental fetch request for session 1
    val session1request2 = createRequest(
      new JFetchMetadata(session1resp.sessionId(), 1),
      new util.LinkedHashMap[TopicPartition, FetchRequest.PartitionData],
      new util.ArrayList[TopicIdPartition], isFromFollower = false)
    val context1v2 = fetchManager.newContext(
      session1request2.version,
      session1request2.metadata,
      session1request2.isFromFollower,
      session1request2.fetchData(topicNames),
      session1request2.forgottenTopics(topicNames),
      topicNames
    )
    assertEquals(classOf[IncrementalFetchContext], context1v2.getClass)

    // total sleep time will now be large enough that fetch session 1 will be evicted if not correctly touched
    time.sleep(501)

    // create one final session to test that the least recently used entry is evicted
    // the second session should be evicted because the first session was incrementally fetched
    // more recently than the second session was created
    val session3req = new util.LinkedHashMap[TopicPartition, FetchRequest.PartitionData]
    session3req.put(foo0.topicPartition, new FetchRequest.PartitionData(fooId, 0, 0, 100,
      Optional.empty()))
    session3req.put(foo1.topicPartition, new FetchRequest.PartitionData(fooId,0, 0, 100,
      Optional.empty()))
    val session3request1 = createRequest(JFetchMetadata.INITIAL, session3req, EMPTY_PART_LIST, isFromFollower = false)
    val session3context = fetchManager.newContext(
      session3request1.version,
      session3request1.metadata,
      session3request1.isFromFollower,
      session3request1.fetchData(topicNames),
      session3request1.forgottenTopics(topicNames),
      topicNames
    )
    assertEquals(classOf[FullFetchContext], session3context.getClass)
    val respData3 = new util.LinkedHashMap[TopicIdPartition, FetchResponseData.PartitionData]
    respData3.put(new TopicIdPartition(fooId, new TopicPartition("foo", 0)), new FetchResponseData.PartitionData()
      .setPartitionIndex(0)
      .setHighWatermark(100)
      .setLastStableOffset(100)
      .setLogStartOffset(100))
    respData3.put(new TopicIdPartition(fooId, new TopicPartition("foo", 1)),
      new FetchResponseData.PartitionData()
        .setPartitionIndex(1)
        .setHighWatermark(10)
        .setLastStableOffset(10)
        .setLogStartOffset(10))
    val session3resp = session3context.updateAndGenerateResponseData(respData3)
    assertEquals(Errors.NONE, session3resp.error())
    assertTrue(session3resp.sessionId() != INVALID_SESSION_ID)
    assertEquals(2, session3resp.responseData(topicNames, session3request1.version).size)

    assertTrue(cacheShard.get(session1resp.sessionId()).isDefined)
    assertFalse(cacheShard.get(session2resp.sessionId()).isDefined, "session 2 should have been evicted by latest session, as session 1 was used more recently")
    assertTrue(cacheShard.get(session3resp.sessionId()).isDefined)
  }

  @Test
  def testPrivilegedSessionHandling(): Unit = {
    val time = new MockTime()
    // set maximum entries to 2 to allow for eviction later
    val cacheShard = new FetchSessionCacheShard(2, 1000)
    val fetchManager = new FetchManager(time, cacheShard)
    val fooId = Uuid.randomUuid()
    val topicNames = Map(fooId -> "foo").asJava
    val foo0 = new TopicIdPartition(fooId, new TopicPartition("foo", 0))
    val foo1 = new TopicIdPartition(fooId, new TopicPartition("foo", 1))

    // Create a new fetch session, session 1
    val session1req = new util.LinkedHashMap[TopicPartition, FetchRequest.PartitionData]
    session1req.put(foo0.topicPartition, new FetchRequest.PartitionData(fooId, 0, 0, 100,
      Optional.empty()))
    session1req.put(foo1.topicPartition, new FetchRequest.PartitionData(fooId, 10, 0, 100,
      Optional.empty()))
    val session1request = createRequest(JFetchMetadata.INITIAL, session1req, EMPTY_PART_LIST, isFromFollower = true)
    val session1context = fetchManager.newContext(
      session1request.version,
      session1request.metadata,
      session1request.isFromFollower,
      session1request.fetchData(topicNames),
      session1request.forgottenTopics(topicNames),
      topicNames
    )
    assertEquals(classOf[FullFetchContext], session1context.getClass)
    val respData1 = new util.LinkedHashMap[TopicIdPartition, FetchResponseData.PartitionData]
    respData1.put(foo0, new FetchResponseData.PartitionData()
        .setPartitionIndex(0)
        .setHighWatermark(100)
        .setLastStableOffset(100)
        .setLogStartOffset(100))
    respData1.put(foo1, new FetchResponseData.PartitionData()
        .setPartitionIndex(1)
        .setHighWatermark(10)
        .setLastStableOffset(10)
        .setLogStartOffset(10))
    val session1resp = session1context.updateAndGenerateResponseData(respData1)
    assertEquals(Errors.NONE, session1resp.error())
    assertTrue(session1resp.sessionId() != INVALID_SESSION_ID)
    assertEquals(2, session1resp.responseData(topicNames, session1request.version).size)
    assertEquals(1, cacheShard.size)

    // move time forward to age session 1 a little compared to session 2
    time.sleep(500)

    // Create a second new fetch session, unprivileged
    val session2req = new util.LinkedHashMap[TopicPartition, FetchRequest.PartitionData]
    session2req.put(foo0.topicPartition, new FetchRequest.PartitionData(fooId, 0, 0, 100,
      Optional.empty()))
    session2req.put(foo1.topicPartition, new FetchRequest.PartitionData(fooId, 10, 0, 100,
      Optional.empty()))
    val session2request = createRequest(JFetchMetadata.INITIAL, session1req, EMPTY_PART_LIST, isFromFollower = false)
    val session2context = fetchManager.newContext(
      session2request.version,
      session2request.metadata,
      session2request.isFromFollower,
      session2request.fetchData(topicNames),
      session2request.forgottenTopics(topicNames),
      topicNames
    )
    assertEquals(classOf[FullFetchContext], session2context.getClass)
    val session2RespData = new util.LinkedHashMap[TopicIdPartition, FetchResponseData.PartitionData]
    session2RespData.put(foo0,
      new FetchResponseData.PartitionData()
        .setPartitionIndex(0)
        .setHighWatermark(100)
        .setLastStableOffset(100)
        .setLogStartOffset(100))
    session2RespData.put(foo1,
      new FetchResponseData.PartitionData()
        .setPartitionIndex(1)
        .setHighWatermark(10)
        .setLastStableOffset(10)
        .setLogStartOffset(10))
    val session2resp = session2context.updateAndGenerateResponseData(session2RespData)
    assertEquals(Errors.NONE, session2resp.error())
    assertTrue(session2resp.sessionId() != INVALID_SESSION_ID)
    assertEquals(2, session2resp.responseData(topicNames, session2request.version).size)

    // both newly created entries are present in cache
    assertTrue(cacheShard.get(session1resp.sessionId()).isDefined)
    assertTrue(cacheShard.get(session2resp.sessionId()).isDefined)
    assertEquals(2, cacheShard.size)
    time.sleep(500)

    // create a session to test session1 privileges mean that session 1 is retained and session 2 is evicted
    val session3req = new util.LinkedHashMap[TopicPartition, FetchRequest.PartitionData]
    session3req.put(foo0.topicPartition, new FetchRequest.PartitionData(fooId, 0, 0, 100,
      Optional.empty()))
    session3req.put(foo1.topicPartition, new FetchRequest.PartitionData(fooId, 0, 0, 100,
      Optional.empty()))
    val session3request = createRequest(JFetchMetadata.INITIAL, session3req, EMPTY_PART_LIST, isFromFollower = true)
    val session3context = fetchManager.newContext(
      session3request.version,
      session3request.metadata,
      session3request.isFromFollower,
      session3request.fetchData(topicNames),
      session3request.forgottenTopics(topicNames),
      topicNames
    )
    assertEquals(classOf[FullFetchContext], session3context.getClass)
    val respData3 = new util.LinkedHashMap[TopicIdPartition, FetchResponseData.PartitionData]
    respData3.put(foo0,
      new FetchResponseData.PartitionData()
        .setPartitionIndex(0)
        .setHighWatermark(100)
        .setLastStableOffset(100)
        .setLogStartOffset(100))
    respData3.put(foo1,
      new FetchResponseData.PartitionData()
        .setPartitionIndex(1)
        .setHighWatermark(10)
        .setLastStableOffset(10)
        .setLogStartOffset(10))
    val session3resp = session3context.updateAndGenerateResponseData(respData3)
    assertEquals(Errors.NONE, session3resp.error())
    assertTrue(session3resp.sessionId() != INVALID_SESSION_ID)
    assertEquals(2, session3resp.responseData(topicNames, session3request.version).size)

    assertTrue(cacheShard.get(session1resp.sessionId()).isDefined)
    // even though session 2 is more recent than session 1, and has not reached expiry time, it is less
    // privileged than session 2, and thus session 3 should be entered and session 2 evicted.
    assertFalse(cacheShard.get(session2resp.sessionId()).isDefined, "session 2 should have been evicted by session 3")
    assertTrue(cacheShard.get(session3resp.sessionId()).isDefined)
    assertEquals(2, cacheShard.size)

    time.sleep(501)

    // create a final session to test whether session1 can be evicted due to age even though it is privileged
    val session4req = new util.LinkedHashMap[TopicPartition, FetchRequest.PartitionData]
    session4req.put(foo0.topicPartition, new FetchRequest.PartitionData(fooId, 0, 0, 100,
      Optional.empty()))
    session4req.put(foo1.topicPartition, new FetchRequest.PartitionData(fooId, 0, 0, 100,
      Optional.empty()))
    val session4request = createRequest(JFetchMetadata.INITIAL, session4req, EMPTY_PART_LIST, isFromFollower = true)
    val session4context = fetchManager.newContext(
      session4request.version,
      session4request.metadata,
      session4request.isFromFollower,
      session4request.fetchData(topicNames),
      session4request.forgottenTopics(topicNames),
      topicNames
    )
    assertEquals(classOf[FullFetchContext], session4context.getClass)
    val respData4 = new util.LinkedHashMap[TopicIdPartition, FetchResponseData.PartitionData]
    respData4.put(foo0,
      new FetchResponseData.PartitionData()
        .setPartitionIndex(0)
        .setHighWatermark(100)
        .setLastStableOffset(100)
        .setLogStartOffset(100))
    respData4.put(foo1,
      new FetchResponseData.PartitionData()
        .setPartitionIndex(1)
        .setHighWatermark(10)
        .setLastStableOffset(10)
        .setLogStartOffset(10))
    val session4resp = session3context.updateAndGenerateResponseData(respData4)
    assertEquals(Errors.NONE, session4resp.error())
    assertTrue(session4resp.sessionId() != INVALID_SESSION_ID)
    assertEquals(2, session4resp.responseData(topicNames, session4request.version).size)

    assertFalse(cacheShard.get(session1resp.sessionId()).isDefined, "session 1 should have been evicted by session 4 even though it is privileged as it has hit eviction time")
    assertTrue(cacheShard.get(session3resp.sessionId()).isDefined)
    assertTrue(cacheShard.get(session4resp.sessionId()).isDefined)
    assertEquals(2, cacheShard.size)
  }

  @Test
  def testZeroSizeFetchSession(): Unit = {
    val time = new MockTime()
    val cacheShard = new FetchSessionCacheShard(10, 1000)
    val fetchManager = new FetchManager(time, cacheShard)
    val fooId = Uuid.randomUuid()
    val topicNames = Map(fooId -> "foo").asJava
    val foo0 = new TopicIdPartition(fooId, new TopicPartition("foo", 0))
    val foo1 = new TopicIdPartition(fooId, new TopicPartition("foo", 1))

    // Create a new fetch session with foo-0 and foo-1
    val reqData1 = new util.LinkedHashMap[TopicPartition, FetchRequest.PartitionData]
    reqData1.put(foo0.topicPartition, new FetchRequest.PartitionData(fooId, 0, 0, 100,
      Optional.empty()))
    reqData1.put(foo1.topicPartition, new FetchRequest.PartitionData(fooId, 10, 0, 100,
      Optional.empty()))
    val request1 = createRequest(JFetchMetadata.INITIAL, reqData1, EMPTY_PART_LIST, isFromFollower = false)
    val context1 = fetchManager.newContext(
      request1.version,
      request1.metadata,
      request1.isFromFollower,
      request1.fetchData(topicNames),
      request1.forgottenTopics(topicNames),
      topicNames
    )
    assertEquals(classOf[FullFetchContext], context1.getClass)
    val respData1 = new util.LinkedHashMap[TopicIdPartition, FetchResponseData.PartitionData]
    respData1.put(foo0, new FetchResponseData.PartitionData()
        .setPartitionIndex(0)
        .setHighWatermark(100)
        .setLastStableOffset(100)
        .setLogStartOffset(100))
    respData1.put(foo1, new FetchResponseData.PartitionData()
        .setPartitionIndex(1)
        .setHighWatermark(10)
        .setLastStableOffset(10)
        .setLogStartOffset(10))
    val resp1 = context1.updateAndGenerateResponseData(respData1)
    assertEquals(Errors.NONE, resp1.error)
    assertTrue(resp1.sessionId() != INVALID_SESSION_ID)
    assertEquals(2, resp1.responseData(topicNames, request1.version).size)

    // Create an incremental fetch request that removes foo-0 and foo-1
    // Verify that the previous fetch session was closed.
    val reqData2 = new util.LinkedHashMap[TopicPartition, FetchRequest.PartitionData]
    val removed2 = new util.ArrayList[TopicIdPartition]
    removed2.add(foo0)
    removed2.add(foo1)
    val request2 = createRequest( new JFetchMetadata(resp1.sessionId, 1), reqData2, removed2, isFromFollower = false)
    val context2 = fetchManager.newContext(
      request2.version,
      request2.metadata,
      request2.isFromFollower,
      request2.fetchData(topicNames),
      request2.forgottenTopics(topicNames),
      topicNames
    )
    assertEquals(classOf[SessionlessFetchContext], context2.getClass)
    val respData2 = new util.LinkedHashMap[TopicIdPartition, FetchResponseData.PartitionData]
    val resp2 = context2.updateAndGenerateResponseData(respData2)
    assertEquals(INVALID_SESSION_ID, resp2.sessionId)
    assertTrue(resp2.responseData(topicNames, request2.version).isEmpty)
    assertEquals(0, cacheShard.size)
  }

  @Test
  def testDivergingEpoch(): Unit = {
    val time = new MockTime()
    val cacheShard = new FetchSessionCacheShard(10, 1000)
    val fetchManager = new FetchManager(time, cacheShard)
    val topicNames = Map(Uuid.randomUuid() -> "foo", Uuid.randomUuid() -> "bar").asJava
    val topicIds = topicNames.asScala.map(_.swap).asJava
    val tp1 = new TopicIdPartition(topicIds.get("foo"), new TopicPartition("foo", 1))
    val tp2 = new TopicIdPartition(topicIds.get("bar"), new TopicPartition("bar", 2))

    val reqData = new util.LinkedHashMap[TopicPartition, FetchRequest.PartitionData]
    reqData.put(tp1.topicPartition, new FetchRequest.PartitionData(tp1.topicId, 100, 0, 1000, Optional.of(5), Optional.of(4)))
    reqData.put(tp2.topicPartition, new FetchRequest.PartitionData(tp2.topicId, 100, 0, 1000, Optional.of(5), Optional.of(4)))

    // Full fetch context returns all partitions in the response
    val request1 = createRequest(JFetchMetadata.INITIAL, reqData, EMPTY_PART_LIST, isFromFollower = false)
    val context1 = fetchManager.newContext(
      request1.version,
      request1.metadata,
      request1.isFromFollower,
      request1.fetchData(topicNames),
      request1.forgottenTopics(topicNames),
      topicNames
    )
    assertEquals(classOf[FullFetchContext], context1.getClass)
    val respData = new util.LinkedHashMap[TopicIdPartition, FetchResponseData.PartitionData]
    respData.put(tp1, new FetchResponseData.PartitionData()
        .setPartitionIndex(tp1.partition)
        .setHighWatermark(105)
        .setLastStableOffset(105)
        .setLogStartOffset(0))
    val divergingEpoch = new FetchResponseData.EpochEndOffset().setEpoch(3).setEndOffset(90)
    respData.put(tp2, new FetchResponseData.PartitionData()
        .setPartitionIndex(tp2.partition)
        .setHighWatermark(105)
        .setLastStableOffset(105)
        .setLogStartOffset(0)
        .setDivergingEpoch(divergingEpoch))
    val resp1 = context1.updateAndGenerateResponseData(respData)
    assertEquals(Errors.NONE, resp1.error)
    assertNotEquals(INVALID_SESSION_ID, resp1.sessionId)
    assertEquals(util.Set.of(tp1.topicPartition, tp2.topicPartition), resp1.responseData(topicNames, request1.version).keySet)

    // Incremental fetch context returns partitions with divergent epoch even if none
    // of the other conditions for return are met.
    val request2 = createRequest(new JFetchMetadata(resp1.sessionId, 1), reqData, EMPTY_PART_LIST, isFromFollower = false)
    val context2 = fetchManager.newContext(
      request2.version,
      request2.metadata,
      request2.isFromFollower,
      request2.fetchData(topicNames),
      request2.forgottenTopics(topicNames),
      topicNames
    )
    assertEquals(classOf[IncrementalFetchContext], context2.getClass)
    val resp2 = context2.updateAndGenerateResponseData(respData)
    assertEquals(Errors.NONE, resp2.error)
    assertEquals(resp1.sessionId, resp2.sessionId)
    assertEquals(Collections.singleton(tp2.topicPartition), resp2.responseData(topicNames, request2.version).keySet)

    // All partitions with divergent epoch should be returned.
    respData.put(tp1, new FetchResponseData.PartitionData()
      .setPartitionIndex(tp1.partition)
      .setHighWatermark(105)
      .setLastStableOffset(105)
      .setLogStartOffset(0)
      .setDivergingEpoch(divergingEpoch))
    val resp3 = context2.updateAndGenerateResponseData(respData)
    assertEquals(Errors.NONE, resp3.error)
    assertEquals(resp1.sessionId, resp3.sessionId)
    assertEquals(util.Set.of(tp1.topicPartition, tp2.topicPartition), resp3.responseData(topicNames, request2.version).keySet)

    // Partitions that meet other conditions should be returned regardless of whether
    // divergingEpoch is set or not.
    respData.put(tp1, new FetchResponseData.PartitionData()
      .setPartitionIndex(tp1.partition)
      .setHighWatermark(110)
      .setLastStableOffset(110)
      .setLogStartOffset(0))
    val resp4 = context2.updateAndGenerateResponseData(respData)
    assertEquals(Errors.NONE, resp4.error)
    assertEquals(resp1.sessionId, resp4.sessionId)
    assertEquals(util.Set.of(tp1.topicPartition, tp2.topicPartition), resp4.responseData(topicNames, request2.version).keySet)
  }

  @Test
  def testDeprioritizesPartitionsWithRecordsOnly(): Unit = {
    val time = new MockTime()
    val cacheShard = new FetchSessionCacheShard(10, 1000)
    val fetchManager = new FetchManager(time, cacheShard)
    val topicIds = Map("foo" -> Uuid.randomUuid(), "bar" -> Uuid.randomUuid(), "zar" -> Uuid.randomUuid()).asJava
    val topicNames = topicIds.asScala.map(_.swap).asJava
    val tp1 = new TopicIdPartition(topicIds.get("foo"), new TopicPartition("foo", 1))
    val tp2 = new TopicIdPartition(topicIds.get("bar"), new TopicPartition("bar", 2))
    val tp3 = new TopicIdPartition(topicIds.get("zar"), new TopicPartition("zar", 3))

    val reqData = new util.LinkedHashMap[TopicIdPartition, FetchRequest.PartitionData]
    reqData.put(tp1, new FetchRequest.PartitionData(tp1.topicId, 100, 0, 1000, Optional.of(5), Optional.of(4)))
    reqData.put(tp2, new FetchRequest.PartitionData(tp2.topicId, 100, 0, 1000, Optional.of(5), Optional.of(4)))
    reqData.put(tp3, new FetchRequest.PartitionData(tp3.topicId, 100, 0, 1000, Optional.of(5), Optional.of(4)))

    // Full fetch context returns all partitions in the response
    val context1 = fetchManager.newContext(ApiKeys.FETCH.latestVersion(), JFetchMetadata.INITIAL, isFollower = false,
     reqData, Collections.emptyList(), topicNames)
    assertEquals(classOf[FullFetchContext], context1.getClass)

    val respData1 = new util.LinkedHashMap[TopicIdPartition, FetchResponseData.PartitionData]
    respData1.put(tp1, new FetchResponseData.PartitionData()
      .setPartitionIndex(tp1.topicPartition.partition)
      .setHighWatermark(50)
      .setLastStableOffset(50)
      .setLogStartOffset(0))
    respData1.put(tp2, new FetchResponseData.PartitionData()
      .setPartitionIndex(tp2.topicPartition.partition)
      .setHighWatermark(50)
      .setLastStableOffset(50)
      .setLogStartOffset(0))
    respData1.put(tp3, new FetchResponseData.PartitionData()
      .setPartitionIndex(tp3.topicPartition.partition)
      .setHighWatermark(50)
      .setLastStableOffset(50)
      .setLogStartOffset(0))

    val resp1 = context1.updateAndGenerateResponseData(respData1)
    assertEquals(Errors.NONE, resp1.error)
    assertNotEquals(INVALID_SESSION_ID, resp1.sessionId)
    assertEquals(util.Set.of(tp1.topicPartition, tp2.topicPartition, tp3.topicPartition), resp1.responseData(topicNames, ApiKeys.FETCH.latestVersion()).keySet())

    // Incremental fetch context returns partitions with changes but only deprioritizes
    // the partitions with records
    val context2 = fetchManager.newContext(ApiKeys.FETCH.latestVersion(), new JFetchMetadata(resp1.sessionId, 1), isFollower = false,
      reqData, Collections.emptyList(), topicNames)
    assertEquals(classOf[IncrementalFetchContext], context2.getClass)

    // Partitions are ordered in the session as per last response
    assertPartitionsOrder(context2, Seq(tp1, tp2, tp3))

    // Response is empty
    val respData2 = new util.LinkedHashMap[TopicIdPartition, FetchResponseData.PartitionData]
    val resp2 = context2.updateAndGenerateResponseData(respData2)
    assertEquals(Errors.NONE, resp2.error)
    assertEquals(resp1.sessionId, resp2.sessionId)
    assertEquals(Collections.emptySet(), resp2.responseData(topicNames, ApiKeys.FETCH.latestVersion()).keySet)

    // All partitions with changes should be returned.
    val respData3 = new util.LinkedHashMap[TopicIdPartition, FetchResponseData.PartitionData]
    respData3.put(tp1, new FetchResponseData.PartitionData()
      .setPartitionIndex(tp1.topicPartition.partition)
      .setHighWatermark(60)
      .setLastStableOffset(50)
      .setLogStartOffset(0))
    respData3.put(tp2, new FetchResponseData.PartitionData()
      .setPartitionIndex(tp2.topicPartition.partition)
      .setHighWatermark(60)
      .setLastStableOffset(50)
      .setLogStartOffset(0)
      .setRecords(MemoryRecords.withRecords(Compression.NONE,
        new SimpleRecord(100, null))))
    respData3.put(tp3, new FetchResponseData.PartitionData()
      .setPartitionIndex(tp3.topicPartition.partition)
      .setHighWatermark(50)
      .setLastStableOffset(50)
      .setLogStartOffset(0))
    val resp3 = context2.updateAndGenerateResponseData(respData3)
    assertEquals(Errors.NONE, resp3.error)
    assertEquals(resp1.sessionId, resp3.sessionId)
    assertEquals(util.Set.of(tp1.topicPartition, tp2.topicPartition), resp3.responseData(topicNames, ApiKeys.FETCH.latestVersion()).keySet)

    // Only the partitions whose returned records in the last response
    // were deprioritized
    assertPartitionsOrder(context2, Seq(tp1, tp3, tp2))
  }

  @Test
  def testCachedPartitionEqualsAndHashCode(): Unit = {
    val topicId = Uuid.randomUuid()
    val topicName = "topic"
    val partition = 0

    val cachedPartitionWithIdAndName = new CachedPartition(topicName, topicId, partition)
    val cachedPartitionWithIdAndNoName = new CachedPartition(null, topicId, partition)
    val cachedPartitionWithDifferentIdAndName = new CachedPartition(topicName, Uuid.randomUuid(), partition)
    val cachedPartitionWithZeroIdAndName = new CachedPartition(topicName, Uuid.ZERO_UUID, partition)
    val cachedPartitionWithZeroIdAndOtherName = new CachedPartition("otherTopic", Uuid.ZERO_UUID, partition)

    // CachedPartitions with valid topic IDs will compare topic ID and partition but not topic name.
    assertEquals(cachedPartitionWithIdAndName, cachedPartitionWithIdAndNoName)
    assertEquals(cachedPartitionWithIdAndName.hashCode, cachedPartitionWithIdAndNoName.hashCode)

    assertNotEquals(cachedPartitionWithIdAndName, cachedPartitionWithDifferentIdAndName)
    assertNotEquals(cachedPartitionWithIdAndName.hashCode, cachedPartitionWithDifferentIdAndName.hashCode)

    assertNotEquals(cachedPartitionWithIdAndName, cachedPartitionWithZeroIdAndName)
    assertNotEquals(cachedPartitionWithIdAndName.hashCode, cachedPartitionWithZeroIdAndName.hashCode)

    // CachedPartitions will null name and valid IDs will act just like ones with valid names
    assertEquals(cachedPartitionWithIdAndNoName, cachedPartitionWithIdAndName)
    assertEquals(cachedPartitionWithIdAndNoName.hashCode, cachedPartitionWithIdAndName.hashCode)

    assertNotEquals(cachedPartitionWithIdAndNoName, cachedPartitionWithDifferentIdAndName)
    assertNotEquals(cachedPartitionWithIdAndNoName.hashCode, cachedPartitionWithDifferentIdAndName.hashCode)

    assertNotEquals(cachedPartitionWithIdAndNoName, cachedPartitionWithZeroIdAndName)
    assertNotEquals(cachedPartitionWithIdAndNoName.hashCode, cachedPartitionWithZeroIdAndName.hashCode)

    // CachedPartition with zero Uuids will compare topic name and partition.
    assertNotEquals(cachedPartitionWithZeroIdAndName, cachedPartitionWithZeroIdAndOtherName)
    assertNotEquals(cachedPartitionWithZeroIdAndName.hashCode, cachedPartitionWithZeroIdAndOtherName.hashCode)

    assertEquals(cachedPartitionWithZeroIdAndName, cachedPartitionWithZeroIdAndName)
    assertEquals(cachedPartitionWithZeroIdAndName.hashCode, cachedPartitionWithZeroIdAndName.hashCode)
  }

  @Test
  def testMaybeResolveUnknownName(): Unit = {
    val namedPartition = new CachedPartition("topic", Uuid.randomUuid(), 0)
    val nullNamePartition1 = new CachedPartition(null, Uuid.randomUuid(), 0)
    val nullNamePartition2 = new CachedPartition(null, Uuid.randomUuid(), 0)

    val topicNames = Map(namedPartition.topicId -> "foo", nullNamePartition1.topicId -> "bar").asJava

    // Since the name is not null, we should not change the topic name.
    // We should never have a scenario where the same ID is used by two topic names, but this is used to test we respect the null check.
    namedPartition.maybeResolveUnknownName(topicNames)
    assertEquals("topic", namedPartition.topic)

    // We will resolve this name as it is in the map and the current name is null.
    nullNamePartition1.maybeResolveUnknownName(topicNames)
    assertEquals("bar", nullNamePartition1.topic)

    // If the ID is not in the map, then we don't resolve the name.
    nullNamePartition2.maybeResolveUnknownName(topicNames)
    assertEquals(null, nullNamePartition2.topic)
  }

  private def assertPartitionsOrder(context: FetchContext, partitions: Seq[TopicIdPartition]): Unit = {
    val partitionsInContext = ArrayBuffer.empty[TopicIdPartition]
    context.foreachPartition { (tp, _) =>
      partitionsInContext += tp
    }
    assertEquals(partitions, partitionsInContext.toSeq)
  }

  @Test
  def testFetchSessionCache_getShardedCache_retrievesCacheFromCorrectSegment(): Unit = {
    // Given
    val numShards = 8
    val sessionIdRange = Int.MaxValue / numShards
    val cacheShards = (0 until numShards).map(shardNum => new FetchSessionCacheShard(10, 1000, sessionIdRange, shardNum))
    val cache = new FetchSessionCache(cacheShards)

    // When
    val cache0 = cache.getCacheShard(sessionIdRange - 1)
    val cache1 = cache.getCacheShard(sessionIdRange)
    val cache2 = cache.getCacheShard(sessionIdRange * 2)

    // Then
    assertEquals(cache0, cacheShards(0))
    assertEquals(cache1, cacheShards(1))
    assertEquals(cache2, cacheShards(2))
    assertThrows(classOf[IndexOutOfBoundsException], () => cache.getCacheShard(sessionIdRange * numShards))
  }

  @Test
  def testFetchSessionCache_RoundRobinsIntoShards(): Unit = {
    // Given
    val numShards = 8
    val sessionIdRange = Int.MaxValue / numShards
    val cacheShards = (0 until numShards).map(shardNum => new FetchSessionCacheShard(10, 1000, sessionIdRange, shardNum))
    val cache = new FetchSessionCache(cacheShards)

    // When / Then
    (0 until numShards*2).foreach { shardNum =>
      assertEquals(cacheShards(shardNum % numShards), cache.getNextCacheShard)
    }
  }

  @Test
  def testFetchSessionCache_RoundRobinsIntoShards_WhenIntegerOverflows(): Unit = {
    // Given
    val maxInteger = Int.MaxValue
    FetchSessionCache.counter.set(maxInteger + 1)
    val numShards = 8
    val sessionIdRange = Int.MaxValue / numShards
    val cacheShards = (0 until numShards).map(shardNum => new FetchSessionCacheShard(10, 1000, sessionIdRange, shardNum))
    val cache = new FetchSessionCache(cacheShards)

    // When / Then
    (0 until numShards*2).foreach { shardNum =>
      assertEquals(cacheShards(shardNum % numShards), cache.getNextCacheShard)
    }
  }
}

object FetchSessionTest {
  def idUsageCombinations: java.util.stream.Stream[Arguments] = {
    val data = new java.util.ArrayList[Arguments]()
    for (startsWithTopicIds <- Array(java.lang.Boolean.TRUE, java.lang.Boolean.FALSE))
      for (endsWithTopicIds <- Array(java.lang.Boolean.TRUE, java.lang.Boolean.FALSE))
        data.add(Arguments.of(startsWithTopicIds, endsWithTopicIds))
    data.stream()
  }
}
