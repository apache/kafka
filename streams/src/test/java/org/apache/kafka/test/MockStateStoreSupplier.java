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
package org.apache.kafka.test;

import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.StateStoreSupplier;

import java.util.Collections;
import java.util.Map;

@Deprecated
public class MockStateStoreSupplier implements StateStoreSupplier {
    private String name;
    private boolean persistent;
    private boolean loggingEnabled;

    public MockStateStoreSupplier(final String name,
                                  final boolean persistent) {
        this(name, persistent, true);
    }

    public MockStateStoreSupplier(final String name,
                                  final boolean persistent,
                                  final boolean loggingEnabled) {
        this.name = name;
        this.persistent = persistent;
        this.loggingEnabled = loggingEnabled;
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public StateStore get() {
        return new MockStateStore(name, persistent);
    }

    @Override
    public Map<String, String> logConfig() {
        return Collections.emptyMap();
    }

    @Override
    public boolean loggingEnabled() {
        return loggingEnabled;
    }
}
