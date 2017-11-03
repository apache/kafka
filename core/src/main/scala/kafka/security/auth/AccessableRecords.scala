package kafka.security.auth

import java.lang

import org.apache.kafka.common.record.Record

trait RecordAuthorizer {
  def authorize(record: Record): Boolean
}

case class AccessableRecordIterator(records: lang.Iterable[Record], recordAuthorizer: RecordAuthorizer) extends Iterator[Record] {
  override def hasNext: Boolean = {
    val currentRecord = internalIterator
    while (currentRecord.hasNext) {
      val record = currentRecord.next()
      if (recordAuthorizer.authorize(record))
        return true
    }
    false
  }

  override def next(): Record = {
    while (internalIterator.hasNext) {
      val record = internalIterator.next()
      if (recordAuthorizer.authorize(record))
        return record
    }
    throw new ArrayIndexOutOfBoundsException
  }

  var internalIterator = records.iterator()
}

case class AccessableRecords(records: lang.Iterable[Record], recordAuthorizer: RecordAuthorizer) extends Iterable[Record] {
  override def iterator = new AccessableRecordIterator(records, recordAuthorizer)
}
