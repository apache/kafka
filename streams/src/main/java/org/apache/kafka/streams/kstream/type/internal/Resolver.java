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

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.type.TypeException;

import java.lang.reflect.GenericArrayType;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
import java.lang.reflect.WildcardType;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class Resolver {

    /**
     * Resolves the type. If something cannot be resolved due to insufficient information, TypeException will be thrown.
     *
     * @param type the type to resolve
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

        } else if (type instanceof GenericArrayType) {
            try {
                GenericArrayType arrayType = (GenericArrayType) type;

                return new ArrayType(resolve(arrayType.getGenericComponentType(), env));

            } catch (TypeException ex) {
                throw new TypeException("failed to resolve type: " + type, ex);
            }
        } else if (type instanceof WildcardType) {
            // Wildcard cannot be resolved
            throw new TypeException("failed to resolve type: " + type);

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

    private static Type[] resolve(Type[] types, Map<TypeVariable, Type> env) throws TypeException {
        Type[] resolved = new Type[types.length];

        for (int i = 0; i < types.length; i++) {
            resolved[i] = resolve(types[i], env);
        }

        return resolved;
    }

    /**
     * Resolves the return type of the method implemented by implementationType
     * @param method the method
     * @param implementationType the type that implements the method
     * @return Type
     * @throws TypeException
     */
    public static Type resolveReturnType(Method method, Type implementationType) throws TypeException {
        Map<TypeVariable, Type> env = resolveTypeArguments(method.getDeclaringClass(), implementationType);

        return resolve(method.getGenericReturnType(), env);
    }

    /**
     * Resolves the return type of the method implemented by implementationType.
     * If the method name is not unique, the first method with the specified name is used.
     *
     * @param interfaceClass
     * @param methodName
     * @param implementationType
     * @return
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
            throw new TypeException("failed to determine the return type of " + methodName + ": method name is not unique");

        return Resolver.resolveReturnType(method, implementationType);
    }


    // resolve type arguments of the interfaceClass from the implementationType
    private static Map<TypeVariable, Type> resolveTypeArguments(Class interfaceClass, Type implementationType) throws TypeException {
        return resolveTypeArguments(interfaceClass, implementationType, Collections.<TypeVariable, Type>emptyMap());
    }

    // resolve type arguments of the interfaceClass from the implementationType
    private static Map<TypeVariable, Type> resolveTypeArguments(Class interfaceClass, Type implementationType, Map<TypeVariable, Type> env) {

        Map<TypeVariable, Type> typeArgs = new HashMap<>();
        Class objClass = null;

        if (implementationType instanceof Class) {
            objClass = (Class) implementationType;

            if (objClass.equals((Class) Object.class))
                return typeArgs;

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
            return typeArgs;

        // go up the class hierarchy

        for (Type superType : objClass.getGenericInterfaces()) {
            Map<TypeVariable, Type> resolvedTypeArgs = resolveTypeArguments(interfaceClass, superType, typeArgs);

            if (resolvedTypeArgs != null)
                return resolvedTypeArgs;
        }

        Type superType = objClass.getGenericSuperclass();
        if (superType != null) {
            Map<TypeVariable, Type> resolvedTypeArgs = resolveTypeArguments(interfaceClass, superType, typeArgs);

            if (resolvedTypeArgs != null)
                return resolvedTypeArgs;
        }

        return typeArgs;
    }

    /**
     * Resolves the element type of iterableType
     * @param iterableType
     * @return Type
     * @throws TypeException
     */
    public static Type resolveElementTypeFromIterableType(Type iterableType) throws TypeException {
        if (iterableType != null) {
            Map<TypeVariable, Type> typeArgs = resolveTypeArguments(Iterable.class, iterableType);

            return resolve(Iterable.class.getTypeParameters()[0], typeArgs);
        } else {
            return null;
        }
    }

    public static Type getRawKeyTypeFromWindowedType(Type type) {
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
