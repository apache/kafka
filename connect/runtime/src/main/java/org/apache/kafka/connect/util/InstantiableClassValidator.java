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
package org.apache.kafka.connect.util;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.utils.Utils;

public class InstantiableClassValidator implements ConfigDef.Validator {

    @Override
    public void ensureValid(String name, Object value) {
        if (value == null) {
            // The value will be null if the class couldn't be found; no point in performing follow-up validation
            return;
        }

        Class<?> cls = (Class<?>) value;
        try {
            Object o = cls.getDeclaredConstructor().newInstance();
            Utils.maybeCloseQuietly(o, o + " (instantiated for preflight validation");
        } catch (NoSuchMethodException | IllegalAccessException e) {
            throw new ConfigException(name, cls.getName(), "Could not find a public no-argument constructor for class" + (e.getMessage() != null ? ": " + e.getMessage() : ""));
        } catch (ReflectiveOperationException | LinkageError | RuntimeException e) {
            throw new ConfigException(name, cls.getName(), "Could not instantiate class" + (e.getMessage() != null ? ": " + e.getMessage() : ""));
        }
    }

    @Override
    public String toString() {
        return "A class with a public, no-argument constructor";
    }
}
