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
package org.apache.kafka.coordinator.group;

import org.apache.kafka.clients.consumer.internals.AutoOffsetResetStrategy;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;

import java.util.Arrays;
import java.util.Locale;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Represents the strategy for resetting offsets in share consumer groups when no previous offset is found
 * for a partition or when an offset is out of range.
 *
 * Supports three strategies:
 * <ul>
 *   <li>{@code EARLIEST} - Reset the offset to the earliest available offset
 *   <li>{@code LATEST} - Reset the offset to the latest available offset
 *   <li>{@code BY_DURATION} - Reset the offset to a timestamp that is the specified duration before the current time
 * </ul>
 */
public class ShareGroupAutoOffsetResetStrategy {

    public static final ShareGroupAutoOffsetResetStrategy EARLIEST = new ShareGroupAutoOffsetResetStrategy(AutoOffsetResetStrategy.EARLIEST, StrategyType.EARLIEST);
    public static final ShareGroupAutoOffsetResetStrategy LATEST = new ShareGroupAutoOffsetResetStrategy(AutoOffsetResetStrategy.LATEST, StrategyType.LATEST);

    public enum StrategyType {
        LATEST, EARLIEST, BY_DURATION;

        @Override
        public String toString() {
            return super.toString().toLowerCase(Locale.ROOT);
        }
    }

    private final AutoOffsetResetStrategy delegate;
    private final StrategyType type;

    private ShareGroupAutoOffsetResetStrategy(AutoOffsetResetStrategy delegate, StrategyType type) {
        this.delegate = delegate;
        this.type = type;
    }

    /**
     * Factory method to create a ShareGroupAutoOffsetResetStrategy from a string representation.
     */
    public static ShareGroupAutoOffsetResetStrategy fromString(String offsetStrategy) {
        AutoOffsetResetStrategy baseStrategy = AutoOffsetResetStrategy.fromString(offsetStrategy);
        AutoOffsetResetStrategy.StrategyType baseType = baseStrategy.type();

        StrategyType shareGroupType;
        switch (baseType) {
            case EARLIEST:
                shareGroupType = StrategyType.EARLIEST;
                break;
            case LATEST:
                shareGroupType = StrategyType.LATEST;
                break;
            case BY_DURATION:
                shareGroupType = StrategyType.BY_DURATION;
                break;
            default:
                throw new IllegalArgumentException("Unsupported strategy for ShareGroup: " + baseType);
        }

        return new ShareGroupAutoOffsetResetStrategy(baseStrategy, shareGroupType);
    }

    /**
     * Returns the share group strategy type.
     */
    public StrategyType type() {
        return type;
    }

    /**
     * Returns the name of the share group offset reset strategy.
     */
    public String name() {
        return type.toString();
    }

    /**
     * Delegates the timestamp calculation to the base strategy.
     * @return the timestamp for the OffsetResetStrategy,
     * if the strategy is EARLIEST or LATEST or duration is provided
     * else return Optional.empty()
     */
    public Long timestamp() {
        return delegate.timestamp().get();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ShareGroupAutoOffsetResetStrategy that = (ShareGroupAutoOffsetResetStrategy) o;
        return type == that.type && Objects.equals(delegate, that.delegate);
    }

    @Override
    public int hashCode() {
        return Objects.hash(delegate, type);
    }

    @Override
    public String toString() {
        return "ShareGroupAutoOffsetResetStrategy{" +
                "type=" + type +
                ", delegate=" + delegate +
                '}';
    }

    /**
     * Factory method for creating EARLIEST strategy.
     */
    public static ShareGroupAutoOffsetResetStrategy earliest() {
        return new ShareGroupAutoOffsetResetStrategy(AutoOffsetResetStrategy.EARLIEST, StrategyType.EARLIEST);
    }

    /**
     * Factory method for creating LATEST strategy.
     */
    public static ShareGroupAutoOffsetResetStrategy latest() {
        return new ShareGroupAutoOffsetResetStrategy(AutoOffsetResetStrategy.LATEST, StrategyType.LATEST);
    }

    public static class Validator implements ConfigDef.Validator {
        @Override
        public void ensureValid(String name, Object value) {
            String offsetStrategy = (String) value;
            try {
                fromString(offsetStrategy);
            } catch (Exception e) {
                throw new ConfigException(name, value, "Invalid value `" + offsetStrategy + "` for configuration " +
                        name + ". The value must be either 'earliest', 'latest' or of the format 'by_duration:<PnDTnHnMn.nS>'.");
            }
        }

        public String toString() {
            String values = Arrays.stream(StrategyType.values())
                .map(strategyType -> {
                    if (strategyType == StrategyType.BY_DURATION) {
                        return strategyType + ":PnDTnHnMn.nS";
                    }
                    return strategyType.toString();
                }).collect(Collectors.joining(", "));
            return "[" + values + "]";
        }
    }
}
