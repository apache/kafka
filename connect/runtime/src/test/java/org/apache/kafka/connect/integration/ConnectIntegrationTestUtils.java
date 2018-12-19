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
package org.apache.kafka.connect.integration;

import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Supplier;

public class ConnectIntegrationTestUtils {

    private static final Logger log = LoggerFactory.getLogger(ConnectIntegrationTestUtils.class);
    private static final long CONNECTOR_SETUP_DURATION_MS = 100;

    public static void waitUntil(Supplier<Boolean> condition, long waitForMillis, String errorMessage) {
        long deadline = System.currentTimeMillis() + waitForMillis;
        long delay = CONNECTOR_SETUP_DURATION_MS / 2;
        while (!condition.get()) {
            long current = System.currentTimeMillis();
            if (System.currentTimeMillis() > deadline) {
                throw new TimeoutException(errorMessage);
            }
            try {
                delay = 2 * delay;
                if (current + delay > deadline) {
                    delay = deadline - current;
                }
                log.debug("Condition not met, sleeping for {} millis.", delay);
                Thread.sleep(delay);
            } catch (InterruptedException e) {
                throw new ConnectException("Thread was interrupted", e);
            }
        }
    }
}
