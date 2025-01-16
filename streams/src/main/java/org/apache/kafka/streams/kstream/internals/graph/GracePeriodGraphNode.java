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

package org.apache.kafka.streams.kstream.internals.graph;

/**
 * Represents a stateful {@link ProcessorGraphNode} where a semantic grace period is defined for the processor
 * and its state.
 */
public class GracePeriodGraphNode<K, V> extends ProcessorGraphNode<K, V> {

    private final long gracePeriod;

    public GracePeriodGraphNode(final String nodeName,
                                final ProcessorParameters<K, V, ?, ?> processorParameters,
                                final long gracePeriod) {
        super(nodeName, processorParameters);
        this.gracePeriod = gracePeriod;
    }

    public long gracePeriod() {
        return gracePeriod;
    }
}
