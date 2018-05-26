/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
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

    public SinkNode(final String name,
                    final String topic,
                    final Serializer<K> keySerializer,
                    final Serializer<V> valSerializer,
                    final StreamPartitioner<? super K, ? super V> partitioner) {
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
    public void addChild(final ProcessorNode<?, ?> child) {
        throw new UnsupportedOperationException("sink node does not allow addChild");
    }

    @SuppressWarnings("unchecked")
    @Override
    public void init(final InternalProcessorContext context) {
        super.init(context);
        this.context = context;

        // if serializers are null, get the default ones from the context
        if (keySerializer == null) {
            keySerializer = (Serializer<K>) context.keySerde().serializer();
        }
        if (valSerializer == null) {
            valSerializer = (Serializer<V>) context.valueSerde().serializer();
        }

        // if value serializers are for {@code Change} values, set the inner serializer when necessary
        if (valSerializer instanceof ChangedSerializer &&
                ((ChangedSerializer) valSerializer).inner() == null) {
            ((ChangedSerializer) valSerializer).setInner(context.valueSerde().serializer());
        }
    }


    @Override
    public void process(final K key, final V value) {
        final RecordCollector collector = ((RecordCollector.Supplier) context).recordCollector();

        final long timestamp = context.timestamp();
        if (timestamp < 0) {
            throw new StreamsException("Invalid (negative) timestamp of " + timestamp + " for output record <" + key + ":" + value + ">.");
        }

        try {
            collector.send(topic, key, value, context.headers(), timestamp, keySerializer, valSerializer, partitioner);
        } catch (final ClassCastException e) {
            final String keyClass = key == null ? "unknown because key is null" : key.getClass().getName();
            final String valueClass = value == null ? "unknown because value is null" : value.getClass().getName();
            throw new StreamsException(
                    String.format("A serializer (key: %s / value: %s) is not compatible to the actual key or value type " +
                                    "(key type: %s / value type: %s). Change the default Serdes in StreamConfig or " +
                                    "provide correct Serdes via method parameters.",
                                    keySerializer.getClass().getName(),
                                    valSerializer.getClass().getName(),
                                    keyClass,
                                    valueClass),
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
    @Override
    public String toString(final String indent) {
        final StringBuilder sb = new StringBuilder(super.toString(indent));
        sb.append(indent).append("\ttopic:\t\t");
        sb.append(topic);
        sb.append("\n");
        return sb.toString();
    }

}
