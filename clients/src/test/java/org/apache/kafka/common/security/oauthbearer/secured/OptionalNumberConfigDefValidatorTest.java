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

package org.apache.kafka.common.security.oauthbearer.secured;

import org.apache.kafka.common.config.ConfigDef.Validator;
import org.apache.kafka.common.config.ConfigException;
import org.junit.jupiter.api.Test;

public class OptionalNumberConfigDefValidatorTest extends ConfigDefValidatorTest {

    @Override
    public Validator createValidator() {
        return new OptionalNumberConfigDefValidator(1, false);
    }

    @Test
    public void testNull() {
        OptionalNumberConfigDefValidator validator = new OptionalNumberConfigDefValidator(1);
        assertThrowsWithMessage(ConfigException.class, () -> ensureValid(validator, null), "must be non-null");
    }

    @Test
    public void testZero() {
        assertThrowsWithMessage(ConfigException.class, () -> ensureValid(0), "must be at least 1");
    }

    @Test
    public void testNegative() {
        assertThrowsWithMessage(ConfigException.class, () -> ensureValid(-1), "must be at least 1");
    }

}
