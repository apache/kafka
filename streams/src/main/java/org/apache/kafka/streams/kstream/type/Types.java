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

import org.apache.kafka.streams.kstream.type.internal.ArrayType;
import org.apache.kafka.streams.kstream.type.internal.ParametricType;

import java.lang.reflect.Type;

public class Types {

    /**
     * Defines an explicit type information. It is used when registering a serializer and/or a deserializer for a type.
     * And it also used to give an explicit return type information to functions given to KStream/KTable methods.
     * <p>
     * Example,
     * <pre>
     *     // assuming that the define method is declared using
     *     // import static org.apache.kafka.streams.kstream.KStreamBuilder.define;
     *
     *     builder.register(define(MyGenericClass.class, String.class),
     *         new MyGenericClassDeSerializer(), new MyGenericClassDeserializer())
     * </pre>
     * </p>
     *
     * @param type the Class instance for this type
     * @param typeArgs type arguments
     * @return Type instance
     * @throws TypeException
     */
    public static Type type(Class<?> type, Type... typeArgs) throws TypeException {
        if (typeArgs != null && typeArgs.length > 0) {
            return new ParametricType(type, typeArgs, null);
        } else {
            return type;
        }
    }

    /**
     * Defines an explicit type information for an array.
     */
    public static Type array(Type componentType) throws TypeException {
        return new ArrayType(componentType);
    }
}
