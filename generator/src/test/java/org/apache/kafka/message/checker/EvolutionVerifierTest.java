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

package org.apache.kafka.message.checker;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static org.apache.kafka.message.checker.CheckerTestUtils.toMessage;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

@Timeout(40)
public class EvolutionVerifierTest {
    @Test
    public void testTopLevelMessageApiKeysDoNotMatch() throws Exception {
        assertEquals("Initial apiKey Optional[62] does not match final apiKey Optional[63]",
            assertThrows(EvolutionException.class,
                () -> EvolutionVerifier.verifyTopLevelMessages(
                    toMessage("{'apiKey':62, 'type': 'request', 'name': 'BrokerRegistrationRequest', " +
                        "'validVersions': '0-2', 'flexibleVersions': '0+', " +
                        "'fields': [{'name': 'BrokerId', 'type': 'int32', 'versions': '0+'}]}"),
                    toMessage("{'apiKey':63, 'type': 'request', 'name': 'BrokerRegistrationRequest', " +
                        "'validVersions': '0-2', 'flexibleVersions': '0+', " +
                        "'fields': [{'name': 'BrokerId', 'type': 'int32', 'versions': '0+'}]}"))).
                            getMessage());
    }

    @Test
    public void testTopLevelMessageTypesDoNotMatch() throws Exception {
        assertEquals("Initial type REQUEST does not match final type RESPONSE",
            assertThrows(EvolutionException.class,
                () -> EvolutionVerifier.verifyTopLevelMessages(
                    toMessage("{'apiKey':62, 'type': 'request', 'name': 'BrokerRegistrationRequest', " +
                        "'validVersions': '0-2', 'flexibleVersions': '0+', " +
                        "'fields': [{'name': 'BrokerId', 'type': 'int32', 'versions': '0+'}]}"),
                    toMessage("{'apiKey':62, 'type': 'response', 'name': 'BrokerRegistrationRequest', " +
                        "'validVersions': '0-2', 'flexibleVersions': '0+', " +
                        "'fields': [{'name': 'BrokerId', 'type': 'int32', 'versions': '0+'}]}"))).
                            getMessage());
    }

    @Test
    public void testFlexibleVersionsIsNotASubset() throws Exception {
        assertEquals("Initial flexibleVersions 0+ must be a subset of final flexibleVersions 1+",
            assertThrows(EvolutionException.class,
                () -> EvolutionVerifier.verifyTopLevelMessages(
                    toMessage("{'apiKey':62, 'type': 'request', 'name': 'BrokerRegistrationRequest', " +
                        "'validVersions': '0-2', 'flexibleVersions': '0+', " +
                        "'fields': [{'name': 'BrokerId', 'type': 'int32', 'versions': '0+'}]}"),
                    toMessage("{'apiKey':62, 'type': 'request', 'name': 'BrokerRegistrationRequest', " +
                        "'validVersions': '0-2', 'flexibleVersions': '1+', " +
                        "'fields': [{'name': 'BrokerId', 'type': 'int32', 'versions': '0+'}]}"))).
                            getMessage());
    }

    @Test
    public void testMaximumVersionOfInitialMessageIsHigher() throws Exception {
        assertEquals("Initial maximum valid version 2 must not be higher than final maximum valid version 1",
            assertThrows(EvolutionException.class,
                () -> EvolutionVerifier.verifyTopLevelMessages(
                    toMessage("{'apiKey':62, 'type': 'request', 'name': 'BrokerRegistrationRequest', " +
                        "'validVersions': '0-2', 'flexibleVersions': '0+', " +
                        "'fields': [{'name': 'BrokerId', 'type': 'int32', 'versions': '0+'}]}"),
                    toMessage("{'apiKey':62, 'type': 'request', 'name': 'BrokerRegistrationRequest', " +
                        "'validVersions': '0-1', 'flexibleVersions': '0+', " +
                        "'fields': [{'name': 'BrokerId', 'type': 'int32', 'versions': '0+'}]}"))).
                    getMessage());
    }

    @Test
    public void testMinimumVersionOfInitialMessageIsHigher() throws Exception {
        assertEquals("Initial minimum valid version 1 must not be higher than final minimum valid version 0",
            assertThrows(EvolutionException.class,
                () -> EvolutionVerifier.verifyTopLevelMessages(
                    toMessage("{'apiKey':62, 'type': 'request', 'name': 'BrokerRegistrationRequest', " +
                        "'validVersions': '1-2', 'flexibleVersions': '0+', " +
                        "'fields': [{'name': 'BrokerId', 'type': 'int32', 'versions': '0+'}]}"),
                    toMessage("{'apiKey':62, 'type': 'request', 'name': 'BrokerRegistrationRequest', " +
                        "'validVersions': '0-2', 'flexibleVersions': '0+', " +
                        "'fields': [{'name': 'BrokerId', 'type': 'int32', 'versions': '0+'}]}"))).
                getMessage());
    }

    @Test
    public void testIncompatibleFieldTypeChange() throws Exception {
        assertEquals("Field type for field2 UserId is int32, but field type for field1 UserId is int64",
            assertThrows(UnificationException.class,
                () -> new EvolutionVerifier(
                    toMessage("{'apiKey':62, 'type': 'request', 'name': 'BrokerRegistrationRequest', " +
                        "'validVersions': '1-2', 'flexibleVersions': '0+', " +
                        "'fields': [" +
                        "{'name': 'BrokerId', 'type': 'int32', 'versions': '0+'}," +
                        "{'name': 'ControllerId', 'type': 'int32', 'versions': '1+'}," +
                        "{'name': 'UserId', 'type': 'int64', 'versions': '2+'}" +
                        "]}"),
                    toMessage("{'apiKey':62, 'type': 'request', 'name': 'BrokerRegistrationRequest', " +
                        "'validVersions': '1-2', 'flexibleVersions': '0+', " +
                        "'fields': [" +
                        "{'name': 'BrokerId', 'type': 'int32', 'versions': '0+'}," +
                        "{'name': 'ControllerId', 'type': 'int32', 'versions': '1+'}," +
                        "{'name': 'UserId', 'type': 'int32', 'versions': '2+'}" +
                        "]}")).
                        verify()).
                        getMessage());
    }

    @Test
    public void testNewFieldAddition() throws Exception {
        new EvolutionVerifier(
            toMessage("{'apiKey':62, 'type': 'request', 'name': 'BrokerRegistrationRequest', " +
                "'validVersions': '1-2', 'flexibleVersions': '0+', " +
                "'fields': [" +
                "{'name': 'BrokerId', 'type': 'int32', 'versions': '0+'}," +
                "{'name': 'ControllerId', 'type': 'int32', 'versions': '1+'}," +
                "{'name': 'UserId', 'type': 'int64', 'versions': '2+'}" +
                "]}"),
            toMessage("{'apiKey':62, 'type': 'request', 'name': 'BrokerRegistrationRequest', " +
                "'validVersions': '1-3', 'flexibleVersions': '0+', " +
                "'fields': [" +
                "{'name': 'BrokerId', 'type': 'int32', 'versions': '0+'}," +
                "{'name': 'ControllerId', 'type': 'int32', 'versions': '1+'}," +
                "{'name': 'NewId', 'type': 'int64', 'versions': '3+'}," +
                "{'name': 'UserId', 'type': 'int64', 'versions': '2+'}" +
                "]}")).
            verify();
    }
}
