package org.apache.kafka.streams.kstream.internals.onetomany;

import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueIterator;

public interface KTableRangeValueGetter<K, V> {
	
	KeyValueIterator<K, V> getRange(K key);
	
	V get(K key);
	
	void init(ProcessorContext context);
	
}
