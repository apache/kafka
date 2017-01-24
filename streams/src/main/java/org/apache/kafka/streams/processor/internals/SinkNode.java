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

package org.apache.kafka.streams.processor.internals;

import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.kstream.internals.ChangedSerializer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StreamPartitioner;

public class SinkNode<K, V> extends ProcessorNode<K, V> {

    private final String topic;
    private Serializer<K> keySerializer;
    private Serializer<V> valSerializer;
    private final StreamPartitioner<? super K, ? super V> partitioner;

    private ProcessorContext context;

    public SinkNode(String name, String topic, Serializer<K> keySerializer, Serializer<V> valSerializer, StreamPartitioner<? super K, ? super V> partitioner) {
        super(name);

        this.topic = topic;
        this.keySerializer = keySerializer;
        this.valSerializer = valSerializer;
        this.partitioner = partitioner;
    }

    /**
     * @throws UnsupportedOperationException if this method adds a child to a sink node
     */
    @Override
    public void addChild(ProcessorNode<?, ?> child) {
        throw new UnsupportedOperationException("sink node does not allow addChild");
    }

    @SuppressWarnings("unchecked")
    @Override
    public void init(ProcessorContext context) {
        super.init(context);
        this.context = context;

        // if serializers are null, get the default ones from the context
        if (this.keySerializer == null) this.keySerializer = (Serializer<K>) context.keySerde().serializer();
        if (this.valSerializer == null) this.valSerializer = (Serializer<V>) context.valueSerde().serializer();

        // if value serializers are for {@code Change} values, set the inner serializer when necessary
        if (this.valSerializer instanceof ChangedSerializer &&
                ((ChangedSerializer) this.valSerializer).inner() == null)
            ((ChangedSerializer) this.valSerializer).setInner(context.valueSerde().serializer());
    }


    @Override
    public void process(final K key, final V value) {
        RecordCollector collector = ((RecordCollector.Supplier) context).recordCollector();

        final long timestamp = context.timestamp();
        if (timestamp < 0) {
            throw new StreamsException("Invalid (negative) timestamp of " + timestamp + " for output record <" + key + ":" + value + ">.");
        }

        try {
            collector.send(topic, key, value, null, timestamp, keySerializer, valSerializer, partitioner);
        } catch (ClassCastException e) {
            throw new StreamsException(
                    String.format("A serializer (key: %s / value: %s) is not compatible to the actual key or value type " +
                                    "(key type: %s / value type: %s). Change the default Serdes in StreamConfig or " +
                                    "provide correct Serdes via method parameters.",
                                    keySerializer.getClass().getName(),
                                    valSerializer.getClass().getName(),
                                    key.getClass().getName(),
                                    value.getClass().getName()),
                    e);
        }
    }

    /**
     * @return a string representation of this node, useful for debugging.
     */
    @Override
    public String toString() {
        return toString("");
    }

    /**
     * @return a string representation of this node starting with the given indent, useful for debugging.
     */
    public String toString(String indent) {
        final StringBuilder sb = new StringBuilder(super.toString(indent));
        sb.append(indent).append("\ttopic:\t\t");
        sb.append(topic);
        sb.append("\n");
        return sb.toString();
    }

}
