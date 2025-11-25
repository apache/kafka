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

package org.apache.kafka.metadata.placement;

import java.util.Collections;
import java.util.Map;
import java.util.function.Predicate;

/**
 * The class defined specification of kafka canary
 * partition meet defined condition should be placed in canary pod
 * The condition is controlled by percentage, which indicates percentage of partitions be canary partition.
 * for example,
 * if percentage is 0.1, partition 9, 19, 29,,, etc will be canary partition
 * if percentage is 0.02, partition 49, 99, 149 will be canary partition
 *
 */
public class CanarySpec {
    protected static final Predicate<Integer> ALWAYS_FALSE = i -> false;

    private final String canaryPodName;
    private final Predicate<Integer> predicate;

    public CanarySpec(String canaryPodName, double percentage) {
        this.canaryPodName = canaryPodName;
        if (percentage > 0.0 && percentage <= 1.0) {
            final int module = (int) Math.floor(1 / percentage);
            predicate = i -> i % module == module - 1;
        } else {
            predicate = ALWAYS_FALSE;
        }
    }

    public Map<String, Predicate<Integer>> toMap() {
        return Collections.singletonMap(canaryPodName, predicate);
    }
}
