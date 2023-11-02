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
package org.apache.kafka.streams.integration;

import org.apache.kafka.streams.state.internals.CompositeReadOnlyKeyValueStoreTest;
import org.apache.kafka.streams.state.internals.CompositeReadOnlySessionStoreTest;
import org.apache.kafka.streams.state.internals.CompositeReadOnlyWindowStoreTest;
import org.apache.kafka.streams.state.internals.GlobalStateStoreProviderTest;
import org.apache.kafka.streams.state.internals.StreamThreadStateStoreProviderTest;
import org.apache.kafka.streams.state.internals.WrappingStoreProviderTest;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/**
 * This suite runs all the tests related to querying StateStores (IQ).
 *
 * It can be used from an IDE to selectively just run these tests.
 *
 * Tests ending in the word "Suite" are excluded from the gradle build because it
 * already runs the component tests individually.
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
                        CompositeReadOnlyKeyValueStoreTest.class,
                        CompositeReadOnlyWindowStoreTest.class,
                        CompositeReadOnlySessionStoreTest.class,
                        GlobalStateStoreProviderTest.class,
                        StreamThreadStateStoreProviderTest.class,
                        WrappingStoreProviderTest.class,
                        QueryableStateIntegrationTest.class,
                    })
public class StoreQuerySuite {
}


