package org.apache.kafka.streams.header;

import org.apache.kafka.common.header.internals.RecordHeaders;

public interface Headers  extends Iterable<Header> {

  RecordHeaders toRecordHeaders();
}
