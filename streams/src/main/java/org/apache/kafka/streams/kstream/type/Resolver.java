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

package org.apache.kafka.streams.kstream.type;

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.serialization.Deserializer;

import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;

public class Resolver {

    /**
     * Construct an instance of TypeWithTypeArgs
     */
    public static TypeWithTypeArgs getTypeWithTypeArgs(Class<?> type, Type[] typeArgs) throws TypeException {
        verify(type, typeArgs);

        return new TypeWithTypeArgs(type, typeArgs);
    }

    /**
     * Infers the type from an instance of Deserializer. This returns null when there is not sufficient information to
     * determine fully resolved type.
     */
    public static Type resolveFromDeserializer(Deserializer<?> deserializer) {
        return getReturnType(deserializer, "deserialize", String.class, byte[].class);
    }

    private static Type getReturnType(Object obj, String methodName, Class... argTypes) {
        try {
            Method method = obj.getClass().getMethod(methodName, argTypes);
            return resolve(method.getGenericReturnType());
        } catch (TypeException ex) {
            return null;
        } catch (NoSuchMethodException ex) {
            throw new KafkaException("internal error", ex);
        }
    }

    public static Type resolve(Type type) throws TypeException {
        if (type instanceof Class) {
            return type;
        } else if (type instanceof TypeWithTypeArgs) {
            return type;
        } else if (type instanceof ParameterizedType) {
            try {
                ParameterizedType parameterizedType = (ParameterizedType) type;
                Class<?> rawType = (Class) parameterizedType.getRawType();
                Type[] convertedTypeArgs = resolve(parameterizedType.getActualTypeArguments());

                return new TypeWithTypeArgs(rawType, convertedTypeArgs);

            } catch (TypeException ex) {
                throw new TypeException("failed to resolve type: " + type, ex);
            }

        } else {
            throw new TypeException("failed to resolve type: " + type);
        }
    }

    private static Type[] resolve(Type[] typeArgs) throws TypeException {
        Type[] converted = new Type[typeArgs.length];

        for (int i = 0; i < typeArgs.length; i++) {
            converted[i] = resolve(typeArgs[i]);
        }

        return converted;
    }

    @SuppressWarnings("unchecked")
    public static void verify(Class type, Type[] typeArgs) throws TypeException {

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
                    if (typeArgs[i] instanceof TypeWithTypeArgs) {
                        typeArgClass = ((TypeWithTypeArgs) typeArgs[i]).rawType;
                    } else if (typeArgs[i] instanceof Class) {
                        typeArgClass = (Class) typeArgs[i];
                    } else {
                        throw new TypeException("unsupported type arguemnt class=" + typeArgs[i].getClass());
                    }

                    if (!upperBoundClass.isAssignableFrom(typeArgClass)) {
                        throw new TypeException("type argument mismatch: type=" + typeArgClass + " upper bound=" + upperBoundClass);
                    }
                }
            }
        }
    }

}
