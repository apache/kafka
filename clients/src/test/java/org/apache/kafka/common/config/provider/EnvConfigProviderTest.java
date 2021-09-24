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
package org.apache.kafka.common.config.provider;

import org.apache.kafka.common.config.ConfigData;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class EnvConfigProviderTest {
    private EnvConfigProvider underTest;

    private static Stream<Arguments> parameters() {
        return Stream.of(
            Arguments.of("EXISTING", "value", "Existing variable should return its value"),
            Arguments.of("EXISTING default", "value", "Existing variable with default should return its value"),
            Arguments.of("NON_EXISTING_ENV", null, "Non-existing variable should return null value"),
            Arguments.of("NON_EXISTING_ENV default", "default", "Non-existing variable with default should return default value")
        );
    }

    @BeforeEach
    public void setup() {
        underTest = new TestEnvConfigProvider();
    }

    @ParameterizedTest
    @MethodSource("parameters")
    public void testEnvConfigProvider(String envNameMaybeWithDefault, String expectedValue, String testCase) {
        // given
        Set<String> set = new HashSet<>();
        set.add(envNameMaybeWithDefault);

        // when
        ConfigData actual = underTest.get(null, set);

        // then
        assertEquals(expectedValue, actual.data().get(envNameMaybeWithDefault));
    }

    private static class TestEnvConfigProvider extends EnvConfigProvider {
        private static final Map<String, String> TEST_ENV_VARS = new HashMap<>();

        static {
            TEST_ENV_VARS.put("EXISTING", "value");
        }

        @Override
        String getEnv(String variable) {
            return TEST_ENV_VARS.get(variable);
        }
    }
}
