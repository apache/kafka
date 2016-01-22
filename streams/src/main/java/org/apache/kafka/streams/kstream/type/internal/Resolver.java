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
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class Resolver {

    /**
     * Resolves the type. If something cannot be resolved due to insufficient information, TypeException will be thrown.
     * @param type
     * @return Type
     * @throws TypeException
     */
    public static Type resolve(Type type) throws TypeException {
        return resolve(type, Collections.<TypeVariable, Type>emptyMap());
    }

    private static Type resolve(Type type, Map<TypeVariable, Type> env) throws TypeException {
        if (type instanceof Class) {
            Class<?> rawType = (Class) type;
            TypeVariable[] typeArgs = rawType.getTypeParameters();

            if (typeArgs == null || typeArgs.length == 0) {
                // No type parameters. check if this is an array type
                if (rawType.isArray()) {
                    return new ArrayType(resolve(rawType.getComponentType(), env));
                } else {
                    return type;
                }
            } else {
                // There are type parameters. resolve them.
                try {
                    Type[] resolvedTypeArgs = resolve(typeArgs, env);

                    return new ParametricType(rawType, resolvedTypeArgs);

                } catch (TypeException ex) {
                    throw new TypeException("failed to resolve type: " + type, ex);
                }
            }
        } else if (type instanceof ParameterizedType) {
            try {
                ParameterizedType parameterizedType = (ParameterizedType) type;
                Class<?> rawType = (Class) parameterizedType.getRawType();
                Type[] resolvedTypeArgs = resolve(parameterizedType.getActualTypeArguments(), env);

                return new ParametricType(rawType, resolvedTypeArgs);

            } catch (TypeException ex) {
                throw new TypeException("failed to resolve type: " + type, ex);
            }
        } else if (type instanceof TypeVariable) {
            // Try to resolve using the environment
            Type resolved = env.get(type);

            if (resolved == null)
                throw new TypeException("failed to resolve type variable: " + type);

            return resolved;

        } else if (type instanceof ParametricType) {
            // This is already resolved.
            return type;

        } else if (type instanceof ArrayType) {
            // This is already resolved.
            return type;

        } else {
            throw new TypeException("failed to resolve type: " + type);
        }
    }

    private static Type[] resolve(Type[] typeArgs, Map<TypeVariable, Type> env) throws TypeException {
        Type[] resolved = new Type[typeArgs.length];

        for (int i = 0; i < typeArgs.length; i++) {
            resolved[i] = resolve(typeArgs[i], env);
        }

        return resolved;
    }

    /**
     * Resolves the return type of the function represented by the functionalInterface
     * @param functionalInterface
     * @param function
     * @return Type, null if not resolved
     * @throws TypeException
     */
    public static Type resolveReturnType(Class functionalInterface, Object function) throws TypeException {
        Method[] methods = functionalInterface.getDeclaredMethods();
        if (methods.length != 1)
            throw new TypeException("internal error: failed to determine the function method");

        Map<TypeVariable, Type> env = resolveTypeArguments(functionalInterface, function.getClass());

        return resolveReturnType(methods[0], env);
    }

    private static Map<TypeVariable, Type> resolveTypeArguments(Class interfaceClass, Type implementationType) throws TypeException {
        return resolveTypeArguments(interfaceClass, implementationType, Collections.<TypeVariable, Type>emptyMap());
    }

    /**
     * Resolves the return type of the method of implementationType implementing interfaceClass
     * @param interfaceClass
     * @param implementationType
     * @return Type, null if not resolved
     * @throws TypeException
     */
    public static Type resolveReturnType(Class interfaceClass, String methodName, Type implementationType) throws TypeException {
        Method[] methods = interfaceClass.getDeclaredMethods();

        Method method = null;
        for (Method m : methods) {
            if (methodName.equals(m.getName())) {
                method = m;
                break;
            }
        }
        if (method == null)
            throw new TypeException("internal error: failed to determine the method");

        Map<TypeVariable, Type> env = resolveTypeArguments(interfaceClass, implementationType);

        return resolveReturnType(method, env);
    }

    // resolve type arguments of the interfaceClass from the implementationType
    private static Map<TypeVariable, Type> resolveTypeArguments(Class interfaceClass, Type implementationType, Map<TypeVariable, Type> env) {

        Map<TypeVariable, Type> typeArgs = new HashMap<>();
        Class objClass = null;

        if (implementationType instanceof Class) {
            objClass = (Class) implementationType;

            if (objClass.equals((Class) Object.class))
                return null;

            if (objClass.equals(interfaceClass))
                return typeArgs;

        } else if (implementationType instanceof ParameterizedType) {
            ParameterizedType parameterizedType = (ParameterizedType) implementationType;
            objClass = (Class) parameterizedType.getRawType();

            if (objClass.equals((Class) Object.class))
                return null;

            TypeVariable[] tVars = objClass.getTypeParameters();
            Type[] tArgs = parameterizedType.getActualTypeArguments();

            for (int i = 0; i < tVars.length; i++) {
                try {
                    Type arg = resolve(tArgs[i], env);
                    typeArgs.put(tVars[i], arg);
                } catch (TypeException ex) {
                    // failed to resolve
                }
            }

            if (objClass.equals(interfaceClass))
                return typeArgs;

        } else if (implementationType instanceof ParametricType) {
            ParametricType parametericType = (ParametricType) implementationType;
            objClass = parametericType.rawType;

            if (objClass.equals((Class) Object.class))
                return null;

            TypeVariable[] tVars = objClass.getTypeParameters();
            Type[] tArgs = parametericType.typeArgs;

            for (int i = 0; i < tVars.length; i++) {
                typeArgs.put(tVars[i], tArgs[i]);
            }

            if (objClass.equals(interfaceClass))
                return typeArgs;
        }

        if (objClass == null)
            return null;

        // go up the class hierarchy

        for (Type sup : objClass.getGenericInterfaces()) {
            Map<TypeVariable, Type> resolvedTypeArgs = resolveTypeArguments(interfaceClass, sup, typeArgs);

            if (resolvedTypeArgs != null)
                return resolvedTypeArgs;
        }

        Type supType = objClass.getGenericSuperclass();
        if (supType != null) {
            Map<TypeVariable, Type> resolvedTypeArgs = resolveTypeArguments(interfaceClass, supType, typeArgs);

            if (resolvedTypeArgs != null)
                return resolvedTypeArgs;
        }

        return null;
    }

    /**
     * Resolves the element type of iterableType
     * @param iterableType
     * @return Type, null if not resolved
     * @throws TypeException
     */
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

    private static Type resolveReturnType(Method method, Map<TypeVariable, Type> env) throws TypeException {
        return resolve(method.getGenericReturnType(), env);
    }

    public static Type getWindowedKeyType(Type type) {
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
