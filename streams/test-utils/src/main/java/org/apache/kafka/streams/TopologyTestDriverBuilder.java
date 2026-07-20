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
import java.util.Objects;
import java.util.Optional;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Fluent builder for a {@link TopologyTestDriver}.
 *
 * <p>This is the entry point for constructing a {@link TopologyTestDriver}, for both
 * single and multi-partition mode. Declare the partition count of each relevant topic, then
 * call {@link #build()}: when at least one declared topic has more than one partition the driver wires
 * its multi-partition task graph; declaring only single-partition topics (or none) keeps the
 * single-flat-task behaviour.</p>
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
    private Properties config = new Properties();
    private Optional<Instant> initialWallClockTime = Optional.empty();
    private final Map<String, Integer> declaredTopics = new HashMap<>();

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
        this.config = Objects.requireNonNull(config, "config cannot be null");
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
        this.initialWallClockTime = Optional.ofNullable(initialWallClockTime);
        return this;
    }

    /**
     * Declare the number of partitions for an input or output topic.
     *
     * @param topicName  the topic to declare
     * @param partitions the number of partitions (must be at least 1)
     * @return this builder
     */
    public TopologyTestDriverBuilder declareTopic(final String topicName, final int partitions) {
        Objects.requireNonNull(topicName, "topicName cannot be null");
        if (partitions < 1) {
            throw new IllegalArgumentException(
                "Partition count must be at least 1 (topic='" + topicName + "', partitions=" + partitions + ").");
        }
        declaredTopics.put(topicName, partitions);
        return this;
    }

    /**
     * Build the driver: construct it, declare all topics, and &mdash; when at least one declared topic has more
     * than one partition &mdash; create the multi-partition task graph.
     *
     * @return a ready-to-use {@link TopologyTestDriver}
     */
    public TopologyTestDriver build() {
        final TopologyTestDriver driver = new TopologyTestDriver(
            topology.internalTopologyBuilder,
            config,
            initialWallClockTime.map(Instant::toEpochMilli).orElseGet(System::currentTimeMillis));
        declaredTopics.forEach(driver::declareTopic);
        if (declaredTopics.values().stream().anyMatch(count -> count > 1)) {
            driver.activateMultiPartitionMode();
        }
        return driver;
    }
}
