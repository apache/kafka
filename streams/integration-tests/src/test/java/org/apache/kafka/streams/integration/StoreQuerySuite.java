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

import org.junit.platform.suite.api.SelectClasses;
import org.junit.platform.suite.api.Suite;

/**
 * This suite runs the integration tests related to querying StateStores (IQ).
 *
 * It can be used from an IDE to selectively just run these integration tests. The unit tests for
 * StateStore querying live in the {@code streams} module; see
 * {@code org.apache.kafka.streams.state.internals.StoreQuerySuite}.
 *
 * Tests ending in the word "Suite" are excluded from the gradle build because it
 * already runs the component tests individually.
 */
@Suite
@SelectClasses({
    QueryableStateIntegrationTest.class,
})
public class StoreQuerySuite {
}
