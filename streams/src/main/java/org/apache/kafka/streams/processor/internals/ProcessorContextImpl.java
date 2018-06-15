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

import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.processor.Cancellable;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.Punctuator;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.To;
import org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl;
import org.apache.kafka.streams.state.internals.ThreadCache;

import java.util.List;

public class ProcessorContextImpl extends AbstractProcessorContext implements RecordCollector.Supplier {

    private final StreamTask task;
    private final RecordCollector collector;
    private final ToInternal toInternal = new ToInternal();
    private final static To SEND_TO_ALL = To.all();

    ProcessorContextImpl(final TaskId id,
                         final StreamTask task,
                         final StreamsConfig config,
                         final RecordCollector collector,
                         final ProcessorStateManager stateMgr,
                         final StreamsMetricsImpl metrics,
                         final ThreadCache cache) {
        super(id, config, metrics, stateMgr, cache);
        this.task = task;
        this.collector = collector;
    }

    public ProcessorStateManager getStateMgr() {
        return (ProcessorStateManager) stateManager;
    }

    @Override
    public RecordCollector recordCollector() {
        return this.collector;
    }

    /**
     * @throws StreamsException if an attempt is made to access this state store from an unknown node
     */
    @Override
    public StateStore getStateStore(final String name) {
        if (currentNode() == null) {
            throw new StreamsException("Accessing from an unknown node");
        }

        final StateStore global = stateManager.getGlobalStore(name);
        if (global != null) {
            return global;
        }

        if (!currentNode().stateStores.contains(name)) {
            throw new StreamsException("Processor " + currentNode().name() + " has no access to StateStore " + name +
                    " as the store is not connected to the processor. If you add stores manually via '.addStateStore()' " +
                    "make sure to connect the added store to the processor by providing the processor name to " +
                    "'.addStateStore()' or connect them via '.connectProcessorAndStateStores()'. " +
                    "DSL users need to provide the store name to '.process()', '.transform()', or '.transformValues()' " +
                    "to connect the store to the corresponding operator. If you do not add stores manually, " +
                    "please file a bug report at https://issues.apache.org/jira/projects/KAFKA.");
        }

        return stateManager.getStore(name);
    }

    @SuppressWarnings("unchecked")
    @Override
    public <K, V> void forward(final K key, final V value) {
        forward(key, value, SEND_TO_ALL);
    }

    @SuppressWarnings({"unchecked", "deprecation"})
    @Override
    public <K, V> void forward(final K key, final V value, final int childIndex) {
        forward(key, value, To.child(((List<ProcessorNode>) currentNode().children()).get(childIndex).name()));
    }

    @SuppressWarnings({"unchecked", "deprecation"})
    @Override
    public <K, V> void forward(final K key, final V value, final String childName) {
        forward(key, value, To.child(childName));
    }

    @SuppressWarnings("unchecked")
    @Override
    public <K, V> void forward(final K key, final V value, final To to) {
        toInternal.update(to);
        if (toInternal.hasTimestamp()) {
            recordContext.setTimestamp(toInternal.timestamp());
        }
        final ProcessorNode previousNode = currentNode();
        try {
            final List<ProcessorNode<K, V>> children = (List<ProcessorNode<K, V>>) currentNode().children();
            final String sendTo = toInternal.child();
            if (sendTo != null) {
                final ProcessorNode child = currentNode().getChild(sendTo);
                if (child == null) {
                    throw new StreamsException("Unknown downstream node: " + sendTo + " either does not exist or is not" +
                            " connected to this processor.");
                }
                forward(child, key, value);
            } else {
                if (children.size() == 1) {
                    final ProcessorNode child = children.get(0);
                    forward(child, key, value);
                } else {
                    for (final ProcessorNode child : children) {
                        forward(child, key, value);
                    }
                }
            }
        } finally {
            setCurrentNode(previousNode);
        }
    }

    @SuppressWarnings("unchecked")
    private <K, V> void forward(final ProcessorNode child,
                                final K key,
                                final V value) {
        setCurrentNode(child);
        child.process(key, value);
    }

    @Override
    public void commit() {
        task.needCommit();
    }

    @Override
    public Cancellable schedule(final long interval, final PunctuationType type, final Punctuator callback) {
        return task.schedule(interval, type, callback);
    }

}
