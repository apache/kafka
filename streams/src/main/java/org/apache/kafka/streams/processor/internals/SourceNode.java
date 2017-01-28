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

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.streams.kstream.internals.ChangedDeserializer;
import org.apache.kafka.streams.processor.ProcessorContext;

import java.util.List;

public class SourceNode<K, V> extends ProcessorNode<K, V> {

    private final List<String> topics;

    private ProcessorContext context;
    private Deserializer<K> keyDeserializer;
    private Deserializer<V> valDeserializer;

    public SourceNode(String name, List<String> topics, Deserializer<K> keyDeserializer, Deserializer<V> valDeserializer) {
        super(name);
        this.topics = topics;
        this.keyDeserializer = keyDeserializer;
        this.valDeserializer = valDeserializer;
    }

    K deserializeKey(String topic, byte[] data) {
        return keyDeserializer.deserialize(topic, data);
    }

    V deserializeValue(String topic, byte[] data) {
        return valDeserializer.deserialize(topic, data);
    }

    @SuppressWarnings("unchecked")
    @Override
    public void init(ProcessorContext context) {
        super.init(context);
        this.context = context;

        // if deserializers are null, get the default ones from the context
        if (this.keyDeserializer == null)
            this.keyDeserializer = (Deserializer<K>) context.keySerde().deserializer();
        if (this.valDeserializer == null)
            this.valDeserializer = (Deserializer<V>) context.valueSerde().deserializer();

        // if value deserializers are for {@code Change} values, set the inner deserializer when necessary
        if (this.valDeserializer instanceof ChangedDeserializer &&
                ((ChangedDeserializer) this.valDeserializer).inner() == null)
            ((ChangedDeserializer) this.valDeserializer).setInner(context.valueSerde().deserializer());
    }


    @Override
    public void process(final K key, final V value) {
        context.forward(key, value);
        nodeMetrics.sourceNodeForwardSensor.record();
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
        sb.append(indent).append("\ttopics:\t\t[");
        for (String topic : topics) {
            sb.append(topic);
            sb.append(", ");
        }
        sb.setLength(sb.length() - 2);  // remove the last comma
        sb.append("]\n");
        return sb.toString();
    }

}
