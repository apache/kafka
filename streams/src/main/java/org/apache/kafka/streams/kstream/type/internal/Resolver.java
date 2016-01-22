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

import org.apache.kafka.streams.kstream.KeyValue;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.type.TypeException;

import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
import java.util.HashMap;
import java.util.Map;

public class Resolver {

    public static Type resolve(Type type) throws TypeException {
        if (type instanceof Class) {
            Class<?> rawType = (Class) type;
            TypeVariable[] typeArgs = rawType.getTypeParameters();

            if (typeArgs == null || typeArgs.length == 0) {
                if (rawType.isArray()) {
                    return new ArrayType(rawType.getComponentType());
                } else {
                    return type;
                }
            } else {
                Type[] resolvedTypeArgs = resolve(typeArgs);
                return new ParametricType(rawType, resolvedTypeArgs);
            }
        } else if (type instanceof ParametricType) {
            return type;
        } else if (type instanceof ParameterizedType) {
            try {
                ParameterizedType parameterizedType = (ParameterizedType) type;
                Class<?> rawType = (Class) parameterizedType.getRawType();
                Type[] resolvedTypeArgs = resolve(parameterizedType.getActualTypeArguments());

                return new ParametricType(rawType, resolvedTypeArgs);

            } catch (TypeException ex) {
                throw new TypeException("failed to resolve type: " + type, ex);
            }

        } else {
            throw new TypeException("failed to resolve type: " + type);
        }
    }

    public static Type resolveReturnType(Class target, Class descendant) throws TypeException {
        Map<TypeVariable, Type> typeArgs = resolveTypeArguments(target, descendant);

        return (typeArgs != null) ? typeArgs.get(getTypeVariableOfReturnType(target)) : null;
    }

    private static Type[] resolve(Type[] typeArgs) throws TypeException {
        Type[] converted = new Type[typeArgs.length];

        for (int i = 0; i < typeArgs.length; i++) {
            converted[i] = resolve(typeArgs[i]);
        }

        return converted;
    }

    public static Map<TypeVariable, Type> resolveTypeArguments(Class target, Type descendant) throws TypeException {
        return resolveTypeArguments(target, descendant, new HashMap<TypeVariable, Type>());
    }

    private static Map<TypeVariable, Type> resolveTypeArguments(Class target, Type descendant, Map<TypeVariable, Type> env) throws TypeException {

        Map<TypeVariable, Type> typeArgs = new HashMap<>();
        Map<TypeVariable, Type> resolvedTypeArgs;
        Class objClass = null;

        if (descendant instanceof Class) {
            objClass = (Class) descendant;

            if (objClass.equals((Class) Object.class))
                return null;

            if (objClass.equals(target))
                return typeArgs;

        } else if (descendant instanceof ParameterizedType) {
            ParameterizedType parameterizedType = (ParameterizedType) descendant;
            objClass = (Class) parameterizedType.getRawType();

            if (objClass.equals((Class) Object.class))
                return null;

            TypeVariable[] tVars = objClass.getTypeParameters();
            Type[] tArgs = parameterizedType.getActualTypeArguments();

            for (int i = 0; i < tVars.length; i++) {
                if (tArgs[i] instanceof TypeVariable) {
                    Type arg = env.get((TypeVariable) tArgs[i]);
                    if (arg != null)
                        typeArgs.put(tVars[i], arg);
                } else if (tArgs[i] instanceof ParameterizedType) {
                    Type arg = resolveParameterizedType((ParameterizedType) tArgs[i], env);
                    if (arg != null)
                        typeArgs.put(tVars[i], arg);
                } else if (tArgs[i] instanceof Class) {
                    typeArgs.put(tVars[i], tArgs[i]);
                }
            }

            if (objClass.equals(target))
                return typeArgs;
        }

        if (objClass == null)
            return null;

        // go up the class hierarchy

        for (Type sup : objClass.getGenericInterfaces()) {
            resolvedTypeArgs = resolveTypeArguments(target, sup, typeArgs);

            if (resolvedTypeArgs != null)
                return resolvedTypeArgs;
        }

        Type supType = objClass.getGenericSuperclass();
        if (supType != null) {
            resolvedTypeArgs = resolveTypeArguments(target, supType, typeArgs);

            if (resolvedTypeArgs != null)
                return resolvedTypeArgs;
        }

        return null;
    }

    private static Type resolveParameterizedType(ParameterizedType type, Map<TypeVariable, Type> envTypeArgs) throws TypeException {
        Type[] typeArgs = type.getActualTypeArguments();
        Type[] resolvedTypeArgs = new Type[typeArgs.length];
        for (int i = 0; i < typeArgs.length; i++) {
            if (typeArgs[i] instanceof Class) {
                resolvedTypeArgs[i] = typeArgs[i];
            } else if (typeArgs[i] instanceof TypeVariable) {
                Type resolvedType = envTypeArgs.get(typeArgs[i]);
                if (resolvedType == null)
                    return null;

                resolvedTypeArgs[i] = resolvedType;
            } else {
                return null;
            }
        }
        return new ParametricType((Class) type.getRawType(), resolvedTypeArgs);
    }

    public static Type resolveElementTypeFromIterableType(Type iterableType) throws TypeException {
        if (iterableType != null) {
            Map<TypeVariable, Type> typeArgs = null;

            if (iterableType instanceof Class) {
                typeArgs = resolveTypeArguments(Iterable.class, iterableType);
            } else if (iterableType instanceof ParametricType) {
                ParametricType ptype = (ParametricType) iterableType;
                TypeVariable[] tVars = ptype.rawType.getTypeParameters();
                Type[] tArgs = ptype.typeArgs;

                Map<TypeVariable, Type> env = new HashMap<>();
                for (int i = 0; i < tVars.length; i++) {
                    env.put(tVars[i], tArgs[i]);
                }

                typeArgs = resolveTypeArguments(Iterable.class, ptype.rawType, env);
            }

            if (typeArgs == null)
                return null;

            return typeArgs.get(Iterable.class.getTypeParameters()[0]);
        } else {
            return null;
        }
    }

    private static final TypeVariable getTypeVariableOfReturnType(Class<?> funcClass) throws TypeException {
        Method[] methods = funcClass.getDeclaredMethods();
        if (methods.length != 1)
            throw new TypeException("internal error: failed to determine the function method");

        Type returnType = methods[0].getGenericReturnType();
        if (!(returnType instanceof TypeVariable))
            throw new TypeException("internal error: failed ot determine the return type variable");

        return (TypeVariable) returnType;
    }


    public static Type isWindowedKeyType(Type type) {
        if (type instanceof ParametricType) {
            ParametricType ptype = (ParametricType) type;
            if (ptype.rawType.equals(Windowed.class))
                return ptype.typeArgs[0];
        }
        return null;
    }

    public static Type getKeyTypeFromKeyValueType(Type type) {
        if (type instanceof ParametricType) {
            ParametricType ptype = (ParametricType) type;
            if (ptype.rawType.equals(KeyValue.class)) {
                return ptype.typeArgs[0];
            }
        }
        return null;
    }

    public static Type getValueTypeFromKeyValueType(Type type) {
        if (type instanceof ParametricType) {
            ParametricType ptype = (ParametricType) type;
            if (ptype.rawType.equals(KeyValue.class)) {
                return ptype.typeArgs[1];
            }
        }
        return null;
    }

}
