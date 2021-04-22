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
package org.apache.kafka.common.config;

import java.util.Arrays;


/**
 * validates that a given input string is a legal enum value
 * @param <E> the enum class
 */
public class EnumValueValidator<E extends Enum<?>> implements ConfigDef.Validator {
    private final Class<E> enumClass;

    public EnumValueValidator(Class<E> enumClass) {
        this.enumClass = enumClass;
    }

    @Override
    public void ensureValid(String name, Object value) {
        if (value == null) {
            //allow nulls
            return;
        }
        String valueStr = (String) value;
        E[] legalValues = enumClass.getEnumConstants();
        for  (E legal : legalValues) {
            //case sensitive on purpose
            if (legal.name().equals(valueStr)) {
                return;
            }
        }
        throw new ConfigException("value \"" + valueStr + "\" for " + name + " is not one of " + Arrays.toString(legalValues));
    }
}
