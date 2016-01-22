/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kafka.streams.kstream.type.internal;

import org.apache.kafka.streams.kstream.type.TypeException;

import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
import java.util.Arrays;

public class ParametricType implements Type {

    final Class<?> rawType;
    final Type[] typeArgs;

    public ParametricType(Class<?> type, Type[] typeArgs) throws TypeException {
        verify(type, typeArgs);

        this.rawType = type;
        this.typeArgs = typeArgs;
    }


    @Override
    public int hashCode() {
        return (rawType.hashCode() * 31) ^ Arrays.hashCode(typeArgs);
    }

    @Override
    public boolean equals(Object other) {
        if (other instanceof ParametricType) {
            ParametricType that = (ParametricType) other;
            return this.rawType.equals(that.rawType) && Arrays.equals(this.typeArgs, that.typeArgs);
        }
        return false;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder(rawType.toString());

        sb.append('<');
        sb.append(typeArgs[0]);
        for (int i = 1; i < typeArgs.length; i++) {
            sb.append(',');
            sb.append(typeArgs[i]);
        }
        sb.append('>');

        return sb.toString();
    }

    @SuppressWarnings("unchecked")
    private static void verify(Class type, Type[] typeArgs) throws TypeException {

        if (type == null)
            throw new TypeException("type missing");

        if (typeArgs == null || typeArgs.length == 0)
            throw new TypeException("type argument missing");

        TypeVariable<Class>[] typeVariables = type.getTypeParameters();

        if (typeArgs.length < typeVariables.length)
            throw new TypeException("type argument mismatch: missing type args");

        if (typeArgs.length > typeVariables.length)
            throw new TypeException("type argument mismatch: excess type args");

        // check type bounds
        for (int i = 0; i < typeVariables.length; i++) {
            for (Type upperBound : typeVariables[i].getBounds()) {
                if (upperBound instanceof Class) {
                    Class<?> upperBoundClass = (Class<?>) upperBound;
                    Class<?> typeArgClass;
                    if (typeArgs[i] == null) {
                        throw new TypeException("type argument is null");
                    } else if (typeArgs[i] instanceof ParametricType) {
                        typeArgClass = ((ParametricType) typeArgs[i]).rawType;
                    } else if (typeArgs[i] instanceof Class) {
                        typeArgClass = (Class) typeArgs[i];
                    } else {
                        throw new TypeException("unsupported type argument class=" + typeArgs[i].getClass());
                    }

                    if (!upperBoundClass.isAssignableFrom(typeArgClass)) {
                        throw new TypeException("type argument mismatch: type=" + typeArgClass + " upper bound=" + upperBoundClass);
                    }
                }
            }
        }
    }

}
