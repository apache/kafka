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
package org.apache.kafka.streams;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;

/**
 * Fluent builder for a {@link TopologyTestDriver}.
 *
 * <p>This is the recommended entry point for constructing a {@link TopologyTestDriver}, for both
 * single- and multi-partition topologies. Declare the partition count of each relevant topic, then
 * call {@link #build()}: when at least one declared topic has more than one partition the driver wires
 * its multi-partition task graph; declaring only single-partition topics (or none) keeps the legacy
 * single-flat-task behaviour. The {@link TopologyTestDriver} constructors remain functional but are
 * deprecated in favour of this builder.</p>
 *
 * <pre>{@code
 * TopologyTestDriver driver = new TopologyTestDriverBuilder(topology)
 *     .withConfig(props)
 *     .withInitialWallClockTime(Instant.ofEpochMilli(0))
 *     .declareTopic("input", 4)
 *     .build();
 * }</pre>
 */
public class TopologyTestDriverBuilder {

    private final Topology topology;
    private final Map<String, Integer> declaredTopics = new LinkedHashMap<>();
    private Properties config;
    private Instant initialWallClockTime;

    /**
     * Start building a driver for the given topology.
     *
     * @param topology the topology to be tested
     */
    public TopologyTestDriverBuilder(final Topology topology) {
        this.topology = Objects.requireNonNull(topology, "topology cannot be null");
    }

    /**
     * Set the configuration passed to the driver. Optional; defaults to empty {@link Properties}.
     *
     * @param config the configuration for the topology
     * @return this builder
     */
    public TopologyTestDriverBuilder withConfig(final Properties config) {
        this.config = config;
        return this;
    }

    /**
     * Set the initial value of the driver's internally mocked wall-clock time. Optional; defaults to
     * the current system time.
     *
     * @param initialWallClockTime the initial mocked wall-clock time
     * @return this builder
     */
    public TopologyTestDriverBuilder withInitialWallClockTime(final Instant initialWallClockTime) {
        this.initialWallClockTime = initialWallClockTime;
        return this;
    }

    /**
     * Declare the number of partitions for an input, output, or internal repartition topic.
     *
     * @param topicName  the topic to declare
     * @param partitions the number of partitions (must be at least 1)
     * @return this builder
     * @throws IllegalArgumentException if {@code partitions} is less than 1, or the topic was already
     *         declared with a different count
     */
    public TopologyTestDriverBuilder declareTopic(final String topicName, final int partitions) {
        Objects.requireNonNull(topicName, "topicName cannot be null");
        if (partitions < 1) {
            throw new IllegalArgumentException(
                "Partition count must be at least 1 (topic='" + topicName + "', partitions=" + partitions + ").");
        }
        final Integer existing = declaredTopics.putIfAbsent(topicName, partitions);
        if (existing != null && existing != partitions) {
            throw new IllegalArgumentException(
                "Topic '" + topicName + "' was already declared with " + existing
                    + " partitions; cannot redeclare with " + partitions + ".");
        }
        return this;
    }

    /**
     * Build the driver: construct it, declare all topics, and—when at least one declared topic has more
     * than one partition—create the multi-partition task graph.
     *
     * @return a ready-to-use {@link TopologyTestDriver}
     */
    public TopologyTestDriver build() {
        final TopologyTestDriver driver = new TopologyTestDriver(
            topology.internalTopologyBuilder,
            config != null ? config : new Properties(),
            initialWallClockTime != null ? initialWallClockTime.toEpochMilli() : System.currentTimeMillis());
        declaredTopics.forEach(driver::declareTopic);
        if (declaredTopics.values().stream().anyMatch(count -> count > 1)) {
            driver.activateMultiPartitionMode();
        }
        return driver;
    }
}
