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

import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.processor.StreamPartitioner;
import org.apache.kafka.streams.processor.TopicNameExtractor;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.processor.internals.metrics.ProcessorNodeMetrics;

import static org.apache.kafka.streams.kstream.internals.WrappingNullableUtils.prepareKeySerializer;
import static org.apache.kafka.streams.kstream.internals.WrappingNullableUtils.prepareValueSerializer;

public class SinkNode<KIn, VIn> extends ProcessorNode<KIn, VIn, Void, Void> {

    private Serializer<KIn> keySerializer;
    private Serializer<VIn> valSerializer;
    private final TopicNameExtractor<KIn, VIn> topicExtractor;
    private final StreamPartitioner<? super KIn, ? super VIn> partitioner;

    private Sensor bytesProducedSensor;
    private Sensor recordsProducedSensor;

    private InternalProcessorContext<Void, Void> context;

    SinkNode(final String name,
             final TopicNameExtractor<KIn, VIn> topicExtractor,
             final Serializer<KIn> keySerializer,
             final Serializer<VIn> valSerializer,
             final StreamPartitioner<? super KIn, ? super VIn> partitioner) {
        super(name);

        this.topicExtractor = topicExtractor;
        this.keySerializer = keySerializer;
        this.valSerializer = valSerializer;
        this.partitioner = partitioner;
    }

    /**
     * @throws UnsupportedOperationException if this method adds a child to a sink node
     */
    @Override
    public void addChild(final ProcessorNode<Void, Void, ?, ?> child) {
        throw new UnsupportedOperationException("sink node does not allow addChild");
    }

    @Override
    public void init(final InternalProcessorContext<Void, Void> context) {
        // It is important to first create the sensor before calling init on the
        // parent object. Otherwise due to backwards compatibility an empty sensor
        // without parent is created with the same name.
        // Once the backwards compatibility is not needed anymore it might be possible to
        // change this.
        bytesProducedSensor = ProcessorNodeMetrics.bytesProducedSensor(
            threadId,
            internalProcessorContext.taskId().toString(),
            name(),
            context.topic(),
            context.metrics()
        );
        recordsProducedSensor = ProcessorNodeMetrics.recordsProducedSensor(
            threadId,
            internalProcessorContext.taskId().toString(),
            name(),
            context.topic(),
            context.metrics()
        );

        super.init(context);
        this.context = context;
        keySerializer = prepareKeySerializer(keySerializer, context, this.name());
        valSerializer = prepareValueSerializer(valSerializer, context, this.name());
    }

    @Override
    public void process(final Record<KIn, VIn> record) {
        final RecordCollector collector = ((RecordCollector.Supplier) context).recordCollector();

        final KIn key = record.key();
        final VIn value = record.value();

        final long timestamp = record.timestamp();

        final ProcessorRecordContext contextForExtraction =
            new ProcessorRecordContext(
                timestamp,
                context.offset(),
                context.partition(),
                context.topic(),
                record.headers()
            );

        final String topic = topicExtractor.extract(key, value, contextForExtraction);

        final int bytesProduced =
            collector.send(topic, key, value, record.headers(), timestamp, keySerializer, valSerializer, partitioner);

        // Just use the cached system time to avoid the clock lookup
        bytesProducedSensor.record(bytesProduced, context.currentSystemTimeMs());
        recordsProducedSensor.record(1, context.currentSystemTimeMs());
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
        sb.append(topicExtractor);
        sb.append("\n");
        return sb.toString();
    }

}
