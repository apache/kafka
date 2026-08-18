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

import org.apache.kafka.common.annotation.InterfaceAudience;

import java.time.Instant;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;

/**
 * Fluent builder for a {@link TopologyTestDriver}.
 *
 * <p>This is the entry point for constructing a {@link TopologyTestDriver}.
 * Configure the builder and call {@link #build()}.
 * The {@link TopologyTestDriver} constructors remain functional but are deprecated in favor of
 * this builder.</p>
 *
 * <pre>{@code
 * TopologyTestDriver driver = new TopologyTestDriverBuilder(topology)
 *     .withConfig(props)
 *     .withInitialWallClockTime(Instant.ofEpochMilli(0))
 *     .build();
 * }</pre>
 */
@InterfaceAudience.Public
public class TopologyTestDriverBuilder {

    private final Topology topology;
    private Properties config = new Properties();
    private Optional<Instant> initialWallClockTime = Optional.empty();

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
     * Build the driver: construct it and apply all declared topic partition counts.
     *
     * @return a ready-to-use {@link TopologyTestDriver}
     */
    public TopologyTestDriver build() {
        return new TopologyTestDriver(
            topology.internalTopologyBuilder,
            config,
            initialWallClockTime.map(Instant::toEpochMilli).orElseGet(System::currentTimeMillis));
    }
}
