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

import java.lang.reflect.GenericArrayType;
import java.lang.reflect.Type;

public class ArrayType implements GenericArrayType {

    final Type componentType;

    public ArrayType(Type componentType) throws TypeException {
        this.componentType = Resolver.resolve(componentType);
    }

    @Override
    public Type getGenericComponentType() {
        return componentType;
    }

    @Override
    public int hashCode() {
        return componentType.hashCode() ^ "ArrayType".hashCode();
    }

    @Override
    public boolean equals(Object other) {
        if (other instanceof ArrayType) {
            ArrayType that = (ArrayType) other;
            return this.componentType.equals(that.componentType);
        }
        return false;
    }

    @Override
    public String toString() {
        return "array<" + componentType.toString() + ">";
    }

}
