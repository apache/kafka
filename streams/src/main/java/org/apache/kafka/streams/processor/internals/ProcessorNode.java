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
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.Punctuator;
import org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.DROPPRED_LATE_RECORDS;
import static org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.PROCESS;
import static org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.SKIPPED_RECORDS;
import static org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.STREAM_PROCESSOR_NODE_METRICS;
import static org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.SUPPRESSION_EMIT_RECORDS;

public class ProcessorNode<K, V> {

    // TODO: 'children' can be removed when #forward() via index is removed
    private final List<ProcessorNode<?, ?>> children;
    private final Map<String, ProcessorNode<?, ?>> childByName;

    private final Processor<K, V> processor;

    final Set<String> stateStores;
    final String name;
    NodeMetrics nodeMetrics;

    public ProcessorNode(final String name) {
        this(name, null, null);
    }

    public ProcessorNode(final String name, final Processor<K, V> processor, final Set<String> stateStores) {
        this.name = name;
        this.processor = processor;
        this.children = new ArrayList<>();
        this.childByName = new HashMap<>();
        this.stateStores = stateStores;
    }

    public final String name() {
        return name;
    }

    public final Processor<K, V> processor() {
        return processor;
    }

    public List<ProcessorNode<?, ?>> children() {
        return children;
    }

    ProcessorNode getChild(final String childName) {
        return childByName.get(childName);
    }

    public void addChild(final ProcessorNode<?, ?> child) {
        children.add(child);
        childByName.put(child.name, child);
    }

    public void init(final InternalProcessorContext context) {
        context.setCurrentNode(this);
        try {
            nodeMetrics = new NodeMetrics(context, context.metrics());
            if (processor != null) {
                processor.init(context);
            }
        } catch (final Exception e) {
            throw new StreamsException(String.format("failed to initialize processor %s", name), e);
        }
    }

    public void close() {
        try {
            if (processor != null) {
                processor.close();
            }
            nodeMetrics.removeAllSensors();
        } catch (final Exception e) {
            throw new StreamsException(String.format("failed to close processor %s", name), e);
        }
    }

    public NodeMetrics nodeMetrics() {
        return Utils.notNull(nodeMetrics);
    }

    public void process(final K key, final V value) {
        processor.process(key, value);
        nodeMetrics.processRateSensor.record();
    }

    public void punctuate(final long timestamp, final Punctuator punctuator) {
        punctuator.punctuate(timestamp);
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
    public String toString(final String indent) {
        final StringBuilder sb = new StringBuilder(indent + name + ":\n");
        if (stateStores != null && !stateStores.isEmpty()) {
            sb.append(indent).append("\tstates:\t\t[");
            for (final String store : stateStores) {
                sb.append(store);
                sb.append(", ");
            }
            sb.setLength(sb.length() - 2);  // remove the last comma
            sb.append("]\n");
        }
        return sb.toString();
    }

    public static final class NodeMetrics {
        private final StreamsMetricsImpl metrics;

        private final Sensor processRateSensor;
        private Sensor skippedRecordsRateSensor;
        private Sensor lateRecordsDropRateSensor;
        private Sensor suppressionEmitRateSensor;

        private final Map<String, String> tagMap;
        private final String processorNodeName;
        private final String taskName;

        NodeMetrics(final InternalProcessorContext context,
                    final StreamsMetricsImpl metrics) {
            this.metrics = metrics;

            this.processorNodeName = context.currentNode().name;
            this.taskName = context.taskId().toString();

            this.tagMap = StreamsMetricsImpl.nodeLevelTagMap(Thread.currentThread().getName(), context.taskId().toString(), processorNodeName);

            if (context.currentNode() instanceof SourceNode) {
                // only register the parent for source nodes
                final Sensor taskLevelSensor = metrics.taskLevelSensor(PROCESS, context.taskId().toString(), Sensor.RecordingLevel.DEBUG);
                processRateSensor = metrics.nodeLevelSensor(PROCESS, processorNodeName, taskName, Sensor.RecordingLevel.DEBUG, taskLevelSensor);
            } else {
                processRateSensor = metrics.nodeLevelSensor(PROCESS, processorNodeName, taskName, Sensor.RecordingLevel.DEBUG);
            }

            StreamsMetricsImpl.addInvocationRateAndCount(processRateSensor, STREAM_PROCESSOR_NODE_METRICS, tagMap, PROCESS);
        }

        Sensor processRateSensor() {
            return processRateSensor;
        }

        public Sensor suppressionEmitRateSensor() {
            if (suppressionEmitRateSensor == null) {
                suppressionEmitRateSensor = metrics.nodeLevelSensor(SUPPRESSION_EMIT_RECORDS, processorNodeName, taskName, Sensor.RecordingLevel.DEBUG);
                StreamsMetricsImpl.addInvocationRateAndCount(suppressionEmitRateSensor, STREAM_PROCESSOR_NODE_METRICS, tagMap, SUPPRESSION_EMIT_RECORDS);
            }

            return suppressionEmitRateSensor;
        }

        public Sensor lateRecordsDropRateSensor() {
            if (lateRecordsDropRateSensor == null) {
                lateRecordsDropRateSensor = metrics.nodeLevelSensor(DROPPRED_LATE_RECORDS, processorNodeName, taskName, Sensor.RecordingLevel.INFO);
                StreamsMetricsImpl.addInvocationRateAndCount(lateRecordsDropRateSensor, STREAM_PROCESSOR_NODE_METRICS, tagMap, DROPPRED_LATE_RECORDS);
            }

            return lateRecordsDropRateSensor;
        }

        public Sensor skippedRecordsRateSensor() {
            if (skippedRecordsRateSensor == null) {
                // keep the task-level parent sensor
                final Sensor taskLevelSensor = metrics.taskLevelSensor(SKIPPED_RECORDS, taskName, Sensor.RecordingLevel.INFO);

                skippedRecordsRateSensor = metrics.nodeLevelSensor(SKIPPED_RECORDS, processorNodeName, taskName, Sensor.RecordingLevel.INFO, taskLevelSensor);
                StreamsMetricsImpl.addInvocationRateAndCount(skippedRecordsRateSensor, STREAM_PROCESSOR_NODE_METRICS, tagMap, SKIPPED_RECORDS);
            }

            return skippedRecordsRateSensor;
        }

        private void removeAllSensors() {
            metrics.removeSensor(processRateSensor);

            if (skippedRecordsRateSensor != null) {
                metrics.removeSensor(skippedRecordsRateSensor);
            }

            if (suppressionEmitRateSensor != null) {
                metrics.removeSensor(suppressionEmitRateSensor);
            }
            if (lateRecordsDropRateSensor != null) {
                metrics.removeSensor(lateRecordsDropRateSensor);
            }
        }
    }
}
