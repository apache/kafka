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

package kafka.test.junit;

import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolver;

/**
 * This resolver is used for supplying any type of object to the test invocation. It does not restrict where the given
 * type can be injected, it simply checks if the requested injection type matches the type given in the constructor. If
 * it matches, the given object is returned.
 *
 * This is useful for injecting helper objects and objects which can be fully initialized before the test lifecycle
 * begins.
 */
public class GenericParameterResolver<T> implements ParameterResolver {

    private final T instance;
    private final Class<T> clazz;

    GenericParameterResolver(T instance, Class<T> clazz) {
        this.instance = instance;
        this.clazz = clazz;
    }

    @Override
    public boolean supportsParameter(ParameterContext parameterContext, ExtensionContext extensionContext) {
        return parameterContext.getParameter().getType().equals(clazz);
    }

    @Override
    public Object resolveParameter(ParameterContext parameterContext, ExtensionContext extensionContext) {
        return instance;
    }
}
