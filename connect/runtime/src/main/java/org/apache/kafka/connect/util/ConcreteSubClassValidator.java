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

import java.lang.reflect.Modifier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class ConcreteSubClassValidator implements ConfigDef.Validator {
    private final Class<?> expectedSuperClass;

    private ConcreteSubClassValidator(Class<?> expectedSuperClass) {
        this.expectedSuperClass = expectedSuperClass;
    }

    public static ConcreteSubClassValidator forSuperClass(Class<?> expectedSuperClass) {
        return new ConcreteSubClassValidator(expectedSuperClass);
    }

    @Override
    public void ensureValid(String name, Object value) {
        if (value == null) {
            // The value will be null if the class couldn't be found; no point in performing follow-up validation
            return;
        }

        Class<?> cls = (Class<?>) value;
        if (!expectedSuperClass.isAssignableFrom(cls)) {
            throw new ConfigException(name, String.valueOf(cls), "Not a " + expectedSuperClass.getSimpleName());
        }

        if (Modifier.isAbstract(cls.getModifiers())) {
            String childClassNames = Stream.of(cls.getClasses())
                    .filter(cls::isAssignableFrom)
                    .filter(c -> !Modifier.isAbstract(c.getModifiers()))
                    .filter(c -> Modifier.isPublic(c.getModifiers()))
                    .map(Class::getName)
                    .collect(Collectors.joining(", "));
            String message = Utils.isBlank(childClassNames) ?
                    "Class is abstract and cannot be created." :
                    "Class is abstract and cannot be created. Did you mean " + childClassNames + "?";
            throw new ConfigException(name, cls.getName(), message);
        }
    }

    @Override
    public String toString() {
        return "A concrete subclass of " + expectedSuperClass.getName();
    }
}
