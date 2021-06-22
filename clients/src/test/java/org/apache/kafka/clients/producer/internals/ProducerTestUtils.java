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
package org.apache.kafka.clients.producer.internals;

import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.assertTrue;

public class ProducerTestUtils {
    private static final int MAX_TRIES = 10;

    static void runUntil(
        Sender sender,
        Supplier<Boolean> condition
    ) {
        runUntil(sender, condition, MAX_TRIES);
    }

    static void runUntil(
        Sender sender,
        Supplier<Boolean> condition,
        int maxTries
    ) {
        int tries = 0;
        while (!condition.get() && tries < maxTries) {
            tries++;
            sender.runOnce();
        }
        assertTrue(condition.get(), "Condition not satisfied after " + maxTries + " tries");
    }
}
