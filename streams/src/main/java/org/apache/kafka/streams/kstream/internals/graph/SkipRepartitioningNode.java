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
 * A specialized {@link ProcessorGraphNode} that explicitly prevents repartitioning in Kafka Streams topologies.
 *
 * <p>The {@code SkipRepartitionNode} is used to indicate that a given {@code KStream} is already correctly partitioned,
 * ensuring that downstream operations do not trigger unnecessary repartitioning.</p>
 *
 * <p>By default, Kafka Streams automatically repartitions data when key-changing operations are performed. However, if
 * the input stream is already partitioned correctly, this node allows users to bypass that behavior, improving
 * performance by reducing overhead.</p>
 *
 * <h2>Restrictions</h2>
 *
 * <ul>
 * <li>This node <b>must not be key-changing</b>. Attempting to set it as key-changing will result in an exception.</li>
 * <li>Should not be used in conjunction with operations that depend on repartitioning, such as joins.</li>
 * </ul>
 *
 * @param <K> the type of keys
 * @param <V> the type of values
 */
public class SkipRepartitioningNode<K, V> extends ProcessorGraphNode<K, V> {

    /**
     * Constructs a {@code SkipRepartitionNode} with the specified node name and processor parameters.
     *
     * @param nodeName            the name of this node in the processor topology
     * @param processorParameters the parameters associated with this processor
     */
    public SkipRepartitioningNode(final String nodeName, final ProcessorParameters<K, V, ?, ?> processorParameters) {
        super(nodeName, processorParameters);
    }

    /**
     * Returns a string representation of this {@code SkipRepartitionNode}.
     *
     * <p>This representation includes the node type and the base class string.</p>
     *
     * @return a string representation of the node
     */
    @Override
    public String toString() {
        return String.format("SkipRepartitionNode{} %s", super.toString());
    }

    /**
     * Indicates that this node does not change the key of the stream.
     *
     * <p>Since {@code SkipRepartitionNode} is designed to explicitly preserve partitioning, this method always returns
     * {@code false}, ensuring that downstream operations do not trigger unnecessary repartitioning.</p>
     *
     * <p>Unlike standard {@link ProcessorGraphNode} implementations, this node enforces partition preservation, meaning
     * any attempt to mark it as key-changing is invalid.</p>
     *
     * @return {@code false}, indicating that this node does not alter keys.
     */
    @Override
    public boolean isKeyChangingOperation() {
        return false;
    }

    /**
     * Prevents marking this node as key-changing.
     *
     * <p>Since {@code SkipRepartitionNode} is designed to preserve partitioning, attempting to set it as key-changing
     * will throw an exception.<p>
     *
     * @param keyChangingOperation ignored parameter (always false)
     *
     * @throws IllegalArgumentException if an attempt is made to mark this node as key-changing
     */
    @Override
    public void setKeyChangingOperation(final boolean keyChangingOperation) {
        if (keyChangingOperation) throw new IllegalArgumentException(
            "SkipRepartitionNode cannot be key-changing as it preserves partitioning."
        );
    }
}
