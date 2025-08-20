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
package org.apache.kafka.coordinator.group.modern;

/**
 * A class which holds two counters. One to count subscription by name and
 * another one to count subscription by regex.
 */
public record SubscriptionCount(int byNameCount, int byRegexCount) {

    @Override
    public String toString() {
        return "SubscriptionCount(" +
            "byNameCount=" + byNameCount +
            ", byRegexCount=" + byRegexCount +
            ')';
    }

    /**
     * Increments the name count by 1; This helper is meant to be used with Map#compute.
     */
    public static SubscriptionCount incNameCount(String key, SubscriptionCount count) {
        if (count == null) {
            return new SubscriptionCount(1, 0);
        } else {
            return new SubscriptionCount(count.byNameCount + 1, count.byRegexCount);
        }
    }

    /**
     * Decrements the name count by 1; This helper is meant to be used with Map#compute.
     */
    public static SubscriptionCount decNameCount(String key, SubscriptionCount count) {
        if (count == null || (count.byNameCount == 1 && count.byRegexCount == 0)) {
            return null;
        } else {
            return new SubscriptionCount(count.byNameCount - 1, count.byRegexCount);
        }
    }

    /**
     * Increments the regex count by 1; This helper is meant to be used with Map#compute.
     */
    public static SubscriptionCount incRegexCount(String key, SubscriptionCount count) {
        if (count == null) {
            return new SubscriptionCount(0, 1);
        } else {
            return new SubscriptionCount(count.byNameCount, count.byRegexCount + 1);
        }
    }

    /**
     * Decrements the regex count by 1; This helper is meant to be used with Map#compute.
     */
    public static SubscriptionCount decRegexCount(String key, SubscriptionCount count) {
        if (count == null || (count.byRegexCount == 1 && count.byNameCount == 0)) {
            return null;
        } else {
            return new SubscriptionCount(count.byNameCount, count.byRegexCount - 1);
        }
    }
}
