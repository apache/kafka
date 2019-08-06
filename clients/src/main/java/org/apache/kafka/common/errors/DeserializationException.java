package org.apache.kafka.common.errors;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.record.Record;

/**
 * DeserializationException is a SerializationException, with the metadata of the partition and record.
 */
public class DeserializationException extends SerializationException {

	private final TopicPartition partition;
	private final Record record;

	public DeserializationException(TopicPartition partition, Record record, RuntimeException e) {
		super(String
				.format("Error deserializing key/value for partition %s at offset %d. If needed, please seek past the record to continue consumption.",
						partition, record.offset()), e);
		this.partition = partition;
		this.record = record;
	}

	/**
	 * The partition during deserializing
	 *
	 * @return
	 */
	public TopicPartition getPartition() {
		return partition;
	}

	/**
	 * The record during deserializing
	 *
	 * @return
	 */
	public Record getRecord() {
		return record;
	}
}
