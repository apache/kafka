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
import org.apache.kafka.streams.processor.Punctuator;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.processor.internals.metrics.ProcessorNodeMetrics;
import org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.maybeMeasureLatency;

public class ProcessorNode<KIn, VIn, KOut, VOut> {

    private final List<ProcessorNode<KOut, VOut, ?, ?>> children;
    private final Map<String, ProcessorNode<KOut, VOut, ?, ?>> childByName;

    private final Processor<KIn, VIn, KOut, VOut> processor;
    private final String name;
    private final Time time;

    public final Set<String> stateStores;

    private InternalProcessorContext internalProcessorContext;
    private String threadId;

    private Sensor processSensor;
    private Sensor punctuateSensor;
    private Sensor destroySensor;
    private Sensor createSensor;

    private boolean closed = true;

    public ProcessorNode(final String name) {
        this(name, (Processor<KIn, VIn, KOut, VOut>) null, null);
    }

    public ProcessorNode(final String name,
                         final Processor<KIn, VIn, KOut, VOut> processor,
                         final Set<String> stateStores) {

        this.name = name;
        this.processor = processor;
        this.children = new ArrayList<>();
        this.childByName = new HashMap<>();
        this.stateStores = stateStores;
        this.time = new SystemTime();
    }

    public ProcessorNode(final String name,
                         final org.apache.kafka.streams.processor.Processor<KIn, VIn> processor,
                         final Set<String> stateStores) {

        this.name = name;
        this.processor = ProcessorAdapter.adapt(processor);
        this.children = new ArrayList<>();
        this.childByName = new HashMap<>();
        this.stateStores = stateStores;
        this.time = new SystemTime();
    }

    public final String name() {
        return name;
    }

    public final Processor<KIn, VIn, KOut, VOut> processor() {
        return processor;
    }

    public List<ProcessorNode<KOut, VOut, ?, ?>> children() {
        return children;
    }

    ProcessorNode<KOut, VOut, ?, ?> getChild(final String childName) {
        return childByName.get(childName);
    }

    public void addChild(final ProcessorNode<KOut, VOut, ?, ?> child) {
        children.add(child);
        childByName.put(child.name, child);
    }

    @SuppressWarnings("unchecked")
    public void init(final InternalProcessorContext context) {
        if (!closed)
            throw new IllegalStateException("The processor is not closed");

        try {
            internalProcessorContext = context;
            initSensors();
            maybeMeasureLatency(
                () -> {
                    if (processor != null) {
                        processor.init((ProcessorContext<KOut, VOut>) context);
                    }
                },
                time,
                createSensor
            );
        } catch (final Exception e) {
            throw new StreamsException(String.format("failed to initialize processor %s", name), e);
        }

        // revived tasks could re-initialize the topology,
        // in which case we should reset the flag
        closed = false;
    }

    private void initSensors() {
        threadId = Thread.currentThread().getName();
        final String taskId = internalProcessorContext.taskId().toString();
        final StreamsMetricsImpl streamsMetrics = internalProcessorContext.metrics();
        processSensor = ProcessorNodeMetrics.processSensor(threadId, taskId, name, streamsMetrics);
        punctuateSensor = ProcessorNodeMetrics.punctuateSensor(threadId, taskId, name, streamsMetrics);
        createSensor = ProcessorNodeMetrics.createSensor(threadId, taskId, name, streamsMetrics);
        destroySensor = ProcessorNodeMetrics.destroySensor(threadId, taskId, name, streamsMetrics);
    }

    public void close() {
        throwIfClosed();

        try {
            maybeMeasureLatency(
                () -> {
                    if (processor != null) {
                        processor.close();
                    }
                },
                time,
                destroySensor
            );
            internalProcessorContext.metrics().removeAllNodeLevelSensors(
                threadId,
                internalProcessorContext.taskId().toString(),
                name
            );
        } catch (final Exception e) {
            throw new StreamsException(String.format("failed to close processor %s", name), e);
        }

        closed = true;
    }

    protected void throwIfClosed() {
        if (closed) {
            throw new IllegalStateException("The processor is already closed");
        }
    }


    public void process(final Record<KIn, VIn> record) {
        throwIfClosed();

        try {
            maybeMeasureLatency(() -> processor.process(record), time, processSensor);
        } catch (final ClassCastException e) {
            final String keyClass = record.key() == null ? "unknown because key is null" : record.key().getClass().getName();
            final String valueClass = record.value() == null ? "unknown because value is null" : record.value().getClass().getName();
            throw new StreamsException(String.format("ClassCastException invoking Processor. Do the Processor's "
                    + "input types match the deserialized types? Check the Serde setup and change the default Serdes in "
                    + "StreamConfig or provide correct Serdes via method parameters. Make sure the Processor can accept "
                    + "the deserialized input of type key: %s, and value: %s.%n"
                    + "Note that although incorrect Serdes are a common cause of error, the cast exception might have "
                    + "another cause (in user code, for example). For example, if a processor wires in a store, but casts "
                    + "the generics incorrectly, a class cast exception could be raised during processing, but the "
                    + "cause would not be wrong Serdes.",
                    keyClass,
                    valueClass),
                e);
        }
    }

    public void punctuate(final long timestamp, final Punctuator punctuator) {
        maybeMeasureLatency(() -> punctuator.punctuate(timestamp), time, punctuateSensor);
    }

    public boolean isTerminalNode() {
        return children.isEmpty();
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
}
