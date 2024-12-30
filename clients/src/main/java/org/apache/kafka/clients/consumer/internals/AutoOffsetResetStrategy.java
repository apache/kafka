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
package org.apache.kafka.clients.consumer.internals;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.requests.ListOffsetsRequest;
import org.apache.kafka.common.utils.Utils;

import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.Locale;
import java.util.Objects;
import java.util.Optional;

public class AutoOffsetResetStrategy {
    public enum StrategyType {
        LATEST, EARLIEST, NONE, BY_DURATION;

        @Override
        public String toString() {
            return super.toString().toLowerCase(Locale.ROOT);
        }
    }

    public static final AutoOffsetResetStrategy EARLIEST = new AutoOffsetResetStrategy(StrategyType.EARLIEST);
    public static final AutoOffsetResetStrategy LATEST = new AutoOffsetResetStrategy(StrategyType.LATEST);
    public static final AutoOffsetResetStrategy NONE = new AutoOffsetResetStrategy(StrategyType.NONE);

    private final StrategyType type;
    private final Optional<Duration> duration;

    private AutoOffsetResetStrategy(StrategyType type) {
        this.type = type;
        this.duration = Optional.empty();
    }

    private AutoOffsetResetStrategy(Duration duration) {
        this.type = StrategyType.BY_DURATION;
        this.duration = Optional.of(duration);
    }

    /**
     *  Returns the AutoOffsetResetStrategy from the given string.
     */
    public static AutoOffsetResetStrategy fromString(String offsetStrategy) {
        if (offsetStrategy == null) {
            throw new IllegalArgumentException("Auto offset reset strategy is null");
        }

        if (StrategyType.BY_DURATION.toString().equals(offsetStrategy)) {
            throw new IllegalArgumentException("<:duration> part is missing in by_duration auto offset reset strategy.");
        }

        if (Arrays.asList(Utils.enumOptions(StrategyType.class)).contains(offsetStrategy)) {
            StrategyType type = StrategyType.valueOf(offsetStrategy.toUpperCase(Locale.ROOT));
            switch (type) {
                case EARLIEST:
                    return EARLIEST;
                case LATEST:
                    return LATEST;
                case NONE:
                    return NONE;
                default:
                    throw new IllegalArgumentException("Unknown auto offset reset strategy: " + offsetStrategy);
            }
        }

        if (offsetStrategy.startsWith(StrategyType.BY_DURATION + ":")) {
            String isoDuration = offsetStrategy.substring(StrategyType.BY_DURATION.toString().length() + 1);
            try {
                Duration duration = Duration.parse(isoDuration);
                if (duration.isNegative()) {
                    throw new IllegalArgumentException("Negative duration is not supported in by_duration offset reset strategy.");
                }
                return new AutoOffsetResetStrategy(duration);
            } catch (Exception e) {
                throw new IllegalArgumentException("Unable to parse duration string in by_duration offset reset strategy.", e);
            }
        }

        throw new IllegalArgumentException("Unknown auto offset reset strategy: " + offsetStrategy);
    }

    /**
     * Returns the offset reset strategy type.
     */
    public StrategyType type() {
        return type;
    }

    /**
     * Returns the name of the offset reset strategy.
     */
    public String name() {
        return type.toString();
    }

    /**
     * Return the timestamp to be used for the ListOffsetsRequest.
     * @return the timestamp for the OffsetResetStrategy,
     * if the strategy is EARLIEST or LATEST or duration is provided
     * else return Optional.empty()
     */
    public Optional<Long> timestamp() {
        if (type == StrategyType.EARLIEST)
            return Optional.of(ListOffsetsRequest.EARLIEST_TIMESTAMP);
        else if (type == StrategyType.LATEST)
            return Optional.of(ListOffsetsRequest.LATEST_TIMESTAMP);
        else if (type == StrategyType.BY_DURATION && duration.isPresent()) {
            Instant now = Instant.now();
            return Optional.of(now.minus(duration.get()).toEpochMilli());
        } else
            return Optional.empty();
    }

    public Optional<Duration> duration() {
        return duration;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AutoOffsetResetStrategy that = (AutoOffsetResetStrategy) o;
        return type == that.type && Objects.equals(duration, that.duration);
    }

    @Override
    public int hashCode() {
        return Objects.hash(type, duration);
    }

    @Override
    public String toString() {
        return "AutoOffsetResetStrategy{" +
                "type=" + type +
                (duration.map(value -> ", duration=" + value).orElse("")) +
                '}';
    }

    public static class Validator implements ConfigDef.Validator {
        @Override
        public void ensureValid(String name, Object value) {
            String offsetStrategy = (String) value;
            try {
                fromString(offsetStrategy);
            } catch (Exception e) {
                throw new ConfigException(name, value, "Invalid value `" + offsetStrategy + "` for configuration " +
                        name + ". The value must be either 'earliest', 'latest', 'none' or of the format 'by_duration:<PnDTnHnMn.nS.>'.");
            }
        }
    }
}
