package kafka.migration

import org.apache.kafka.raft.{BatchReader, RaftClient}
import org.apache.kafka.server.common.ApiMessageAndVersion
import org.apache.kafka.snapshot.SnapshotReader

class OffsetTrackingListener extends RaftClient.Listener[ApiMessageAndVersion] {
  @volatile var _highestOffset = 0L

  def highestOffset: Long = _highestOffset

  override def handleCommit(reader: BatchReader[ApiMessageAndVersion]): Unit = {
    reader.lastOffset()
    var index = 0
    while (reader.hasNext) {
      index += 1
      reader.next()
    }
    _highestOffset = reader.lastOffset().orElse(reader.baseOffset() + index)
    reader.close()
  }

  override def handleSnapshot(reader: SnapshotReader[ApiMessageAndVersion]): Unit = {
    _highestOffset = reader.lastContainedLogOffset()
    reader.close()
  }
}
