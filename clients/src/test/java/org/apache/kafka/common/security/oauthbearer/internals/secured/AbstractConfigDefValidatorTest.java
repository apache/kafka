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

package org.apache.kafka.common.security.oauthbearer.internals.secured;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.kafka.common.config.ConfigDef.Validator;
import org.apache.kafka.common.config.ConfigException;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.junit.jupiter.api.function.Executable;

@TestInstance(Lifecycle.PER_CLASS)
public abstract class AbstractConfigDefValidatorTest {

    protected Validator validator;

    @BeforeAll
    public void setup() {
        validator = createValidator();
    }

    protected abstract Validator createValidator();

    @Test
    public void testNull() {
        ensureValid(null);
    }

    @Test
    public void testEmptyString() {
        ensureValid("");
    }

    @Test
    public void testWhitespace() {
        ensureValid("    ");
    }

    protected void assertThrowsWithMessage(Executable validatorExecutable, String substring) {
        assertThrows(ConfigException.class, validatorExecutable);

        try {
            validatorExecutable.execute();
        } catch (Throwable e) {
            assertTrue(e.getMessage().contains(substring), String.format("Expected exception message (\"%s\") to contain substring (\"%s\")", e.getMessage(), substring));
        }
    }

    protected void ensureValid(String value) {
        validator.ensureValid("fakeconfigname", value);
    }

}
