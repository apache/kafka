package kafka.log.remote
import java.io.IOException
import java.util

import kafka.log.LogSegment
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.record.Records

/**
 * A wrapper class of RemoteStorageManager that sets the context class loader when calling RSM methods
 */
class RemoteStorageManagerWrapper(val rsm: RemoteStorageManager, val rsmClassLoader: ClassLoader) extends RemoteStorageManager {

  def withClassLoader[T](fun: => T): T = {
    val originalClassLoader = Thread.currentThread.getContextClassLoader
    Thread.currentThread.setContextClassLoader(rsmClassLoader)
    try {
      fun
    } finally {
      Thread.currentThread.setContextClassLoader(originalClassLoader)
    }
  }

  @throws(classOf[IOException])
  override def earliestLogOffset(tp: TopicPartition): Long = {
    withClassLoader { rsm.earliestLogOffset(tp) }
  }

  @throws(classOf[IOException])
  override def copyLogSegment(topicPartition: TopicPartition, logSegment: LogSegment, leaderEpoch: Int): util.List[RemoteLogIndexEntry] = {
    withClassLoader { rsm.copyLogSegment(topicPartition, logSegment, leaderEpoch) }
  }

  @throws(classOf[IOException])
  override def listRemoteSegments(topicPartition: TopicPartition): util.List[RemoteLogSegmentInfo] = {
    withClassLoader { rsm.listRemoteSegments(topicPartition) }
  }

  @throws(classOf[IOException])
  override def listRemoteSegments(topicPartition: TopicPartition, minBaseOffset: Long): util.List[RemoteLogSegmentInfo] = {
    withClassLoader { rsm.listRemoteSegments(topicPartition, minBaseOffset) }
  }

  @throws(classOf[IOException])
  override def getRemoteLogIndexEntries(remoteLogSegment: RemoteLogSegmentInfo): util.List[RemoteLogIndexEntry] = {
    withClassLoader { rsm.getRemoteLogIndexEntries(remoteLogSegment) }
  }

  @throws(classOf[IOException])
  override def deleteLogSegment(remoteLogSegmentInfo: RemoteLogSegmentInfo): Boolean = {
    withClassLoader { rsm.deleteLogSegment(remoteLogSegmentInfo) }
  }

  @throws(classOf[IOException])
  override def deleteTopicPartition(topicPartition: TopicPartition): Boolean = {
    withClassLoader { rsm.deleteTopicPartition(topicPartition) }
  }

  @throws(classOf[IOException])
  override def cleanupLogUntil(topicPartition: TopicPartition, cleanUpTillMs: Long): Long = {
    withClassLoader { rsm.cleanupLogUntil(topicPartition, cleanUpTillMs) }
  }

  @throws(classOf[IOException])
  override def read(remoteLogIndexEntry: RemoteLogIndexEntry, maxBytes: Int, startOffset: Long, minOneMessage: Boolean): Records = {
    withClassLoader { rsm.read(remoteLogIndexEntry, maxBytes, startOffset, minOneMessage) }
  }

  override def close(): Unit = {
    withClassLoader { rsm.close() }
  }

  override def configure(configs: util.Map[String, _]): Unit = {
    withClassLoader { rsm.configure(configs) }
  }
}
