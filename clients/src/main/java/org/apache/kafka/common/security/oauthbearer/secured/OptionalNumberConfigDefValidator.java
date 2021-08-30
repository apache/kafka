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

/**
 * An implementation of {@link Validator} that, if a value
 * is supplied, is assumed to:
 *
 * <li>
 *     <ul>be a Number</ul>
 *     <ul>have a value that is not less than the provided minimum value</ul>
 * </li>
 *
 * If the value is null or an empty string, it is assumed to be an "empty" value and thus ignored.
 * Any whitespace is trimmed off of the beginning and end.
 *
 * No effort is made to contact the URL in the validation step.
 */

public class OptionalNumberConfigDefValidator implements Validator {

    private final Number min;

    private final boolean isRequired;

    public OptionalNumberConfigDefValidator(Number min) {
        this(min, true);
    }

    public OptionalNumberConfigDefValidator(Number min, boolean isRequired) {
        this.min = min;
        this.isRequired = isRequired;
    }

    @Override
    public void ensureValid(String name, Object value) {
        if (value == null || value.toString().trim().isEmpty()) {
            if (isRequired)
                throw new ConfigException(name, value, String.format("The OAuth configuration option %s must be non-null", name));
            else
                return;
        }

        Number n = (Number) value;

        if (n.doubleValue() < min.doubleValue())
            throw new ConfigException(name, value, String.format("The OAuth configuration option %s value must be at least %s", name, min));
    }

}
