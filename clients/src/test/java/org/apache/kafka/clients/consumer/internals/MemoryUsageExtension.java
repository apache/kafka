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

import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

public class MemoryUsageExtension implements BeforeAllCallback, AfterAllCallback {
    private long memoryBefore;
    private long memoryAfter;

    @Override
    public void beforeAll(ExtensionContext context) {
        System.out.println("Starting test: " + context.getDisplayName());
        memoryBefore = getUsedMemory();
        System.out.println("Memory before: " + memoryBefore + " bytes");
    }

    @Override
    public void afterAll(ExtensionContext context) {
        memoryAfter = getUsedMemory();
        System.out.println("Memory after tests: " + memoryAfter + " bytes");
        long memoryUsed = memoryAfter - memoryBefore;
        System.out.println("Total memory used by tests: " + memoryUsed + " bytes");
    }

    private long getUsedMemory() {
        Runtime runtime = Runtime.getRuntime();
        runtime.gc();
        return runtime.totalMemory() - runtime.freeMemory();
    }
}