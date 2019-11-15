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

import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * repartition topic config which doesn't permit changing number of partitions
 */
public class ImmutableRepartitionTopicConfig extends RepartitionTopicConfig {
    private final int numberOfPartitions;

    ImmutableRepartitionTopicConfig(final String name,
                                    final int numberOfPartitions,
                                    final Map<String, String> topicConfigs) {
        super(name, topicConfigs);
        validateNumberOfPartitions(numberOfPartitions);
        this.numberOfPartitions = numberOfPartitions;
    }

    @Override
    public Optional<Integer> numberOfPartitions() {
        return Optional.of(numberOfPartitions);
    }

    @Override
    public void setNumberOfPartitions(final int numberOfPartitions) {
        //no-op
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final ImmutableRepartitionTopicConfig that = (ImmutableRepartitionTopicConfig) o;
        return Objects.equals(name, that.name) &&
               Objects.equals(topicConfigs, that.topicConfigs) &&
               Objects.equals(numberOfPartitions, that.numberOfPartitions);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, topicConfigs, numberOfPartitions);
    }

    @Override
    public String toString() {
        return "ImmutableRepartitionTopicConfig(" +
               "name=" + name +
               ", topicConfigs=" + topicConfigs +
               ", numberOfPartitions=" + numberOfPartitions +
               ")";
    }
}
