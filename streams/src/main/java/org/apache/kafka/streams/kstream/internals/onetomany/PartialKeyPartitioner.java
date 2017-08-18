package org.apache.kafka.streams.kstream.internals.onetomany;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.apache.kafka.streams.processor.StreamPartitioner;

public class PartialKeyPartitioner<K,V,K1> implements StreamPartitioner<K, V> {

	private ValueMapper<K, K1> extractor;
	private Serializer<K1> keySerializer;
	private String topic;
	
	public PartialKeyPartitioner(ValueMapper<K, K1> extractor, Serde<K1> keySerde, String topic){
		this.keySerializer = keySerde.serializer();
		this.extractor = extractor;
		this.topic = topic;
	}
	
    @Override
    public Integer partition(K key, V value, int numPartitions)
    {
    	return Utils.murmurPartition(keySerializer.serialize(topic, extractor.apply(key)), numPartitions);
    }
}
