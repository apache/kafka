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
import org.apache.kafka.common.utils.SystemTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.Punctuator;
import org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.PROCESSOR_NODE_ID_TAG;
import static org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.PROCESSOR_NODE_METRICS_GROUP;
import static org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.addAvgMaxLatency;
import static org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.addInvocationRateAndCount;

public class ProcessorNode<K, V> {

    // TODO: 'children' can be removed when #forward() via index is removed
    private final List<ProcessorNode<?, ?>> children;
    private final Map<String, ProcessorNode<?, ?>> childByName;

    private NodeMetrics nodeMetrics;
    private final Processor<K, V> processor;
    private final String name;
    private final Time time;

    public final Set<String> stateStores;

    public ProcessorNode(final String name) {
        this(name, null, null);
    }


    public ProcessorNode(final String name, final Processor<K, V> processor, final Set<String> stateStores) {
        this.name = name;
        this.processor = processor;
        this.children = new ArrayList<>();
        this.childByName = new HashMap<>();
        this.stateStores = stateStores;
        this.time = new SystemTime();
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
        try {
            nodeMetrics = new NodeMetrics(context.metrics(), name, context);
            final long startNs = time.nanoseconds();
            if (processor != null) {
                processor.init(context);
            }
            nodeMetrics.nodeCreationSensor.record(time.nanoseconds() - startNs);
        } catch (final Exception e) {
            throw new StreamsException(String.format("failed to initialize processor %s", name), e);
        }
    }

    public void close() {
        try {
            final long startNs = time.nanoseconds();
            if (processor != null) {
                processor.close();
            }
            nodeMetrics.nodeDestructionSensor.record(time.nanoseconds() - startNs);
            nodeMetrics.removeAllSensors();
        } catch (final Exception e) {
            throw new StreamsException(String.format("failed to close processor %s", name), e);
        }
    }


    public void process(final K key, final V value) {
        final long startNs = time.nanoseconds();
        processor.process(key, value);
        nodeMetrics.nodeProcessTimeSensor.record(time.nanoseconds() - startNs);
    }

    public void punctuate(final long timestamp, final Punctuator punctuator) {
        final long startNs = time.nanoseconds();
        punctuator.punctuate(timestamp);
        nodeMetrics.nodePunctuateTimeSensor.record(time.nanoseconds() - startNs);
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

    Sensor sourceNodeForwardSensor() {
        return nodeMetrics.sourceNodeForwardSensor;
    }

    private static final class NodeMetrics {
        private final StreamsMetricsImpl metrics;

        private final Sensor nodeProcessTimeSensor;
        private final Sensor nodePunctuateTimeSensor;
        private final Sensor sourceNodeForwardSensor;
        private final Sensor nodeCreationSensor;
        private final Sensor nodeDestructionSensor;
        private final String taskName;
        private final String processorNodeName;

        private NodeMetrics(final StreamsMetricsImpl metrics, final String processorNodeName, final ProcessorContext context) {
            this.metrics = metrics;

            final String taskName = context.taskId().toString();
            final Map<String, String> tagMap = metrics.tagMap("task-id", context.taskId().toString(), PROCESSOR_NODE_ID_TAG, processorNodeName);
            final Map<String, String> allTagMap = metrics.tagMap("task-id", context.taskId().toString(), PROCESSOR_NODE_ID_TAG, "all");

            nodeProcessTimeSensor = createTaskAndNodeLatencyAndThroughputSensors(
                "process",
                metrics,
                taskName,
                processorNodeName,
                allTagMap,
                tagMap
            );

            nodePunctuateTimeSensor = createTaskAndNodeLatencyAndThroughputSensors(
                "punctuate",
                metrics,
                taskName,
                processorNodeName,
                allTagMap,
                tagMap
            );

            nodeCreationSensor = createTaskAndNodeLatencyAndThroughputSensors(
                "create",
                metrics,
                taskName,
                processorNodeName,
                allTagMap,
                tagMap
            );

            // note: this metric can be removed in the future, as it is only recorded before being immediately removed
            nodeDestructionSensor = createTaskAndNodeLatencyAndThroughputSensors(
                "destroy",
                metrics,
                taskName,
                processorNodeName,
                allTagMap,
                tagMap
            );

            sourceNodeForwardSensor = createTaskAndNodeLatencyAndThroughputSensors(
                "forward",
                metrics,
                taskName,
                processorNodeName,
                allTagMap,
                tagMap
            );

            this.taskName = taskName;
            this.processorNodeName = processorNodeName;
        }

        private void removeAllSensors() {
            metrics.removeAllNodeLevelSensors(taskName, processorNodeName);
        }

        private static Sensor createTaskAndNodeLatencyAndThroughputSensors(final String operation,
                                                                           final StreamsMetricsImpl metrics,
                                                                           final String taskName,
                                                                           final String processorNodeName,
                                                                           final Map<String, String> taskTags,
                                                                           final Map<String, String> nodeTags) {
            final Sensor parent = metrics.taskLevelSensor(taskName, operation, Sensor.RecordingLevel.DEBUG);
            addAvgMaxLatency(parent, PROCESSOR_NODE_METRICS_GROUP, taskTags, operation);
            addInvocationRateAndCount(parent, PROCESSOR_NODE_METRICS_GROUP, taskTags, operation);

            final Sensor sensor = metrics.nodeLevelSensor(taskName, processorNodeName, operation, Sensor.RecordingLevel.DEBUG, parent);
            addAvgMaxLatency(sensor, PROCESSOR_NODE_METRICS_GROUP, nodeTags, operation);
            addInvocationRateAndCount(sensor, PROCESSOR_NODE_METRICS_GROUP, nodeTags, operation);

            return sensor;
        }
    }
}
