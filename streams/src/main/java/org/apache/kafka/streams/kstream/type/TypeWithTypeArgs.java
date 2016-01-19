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

import java.util.Arrays;

class TypeWithTypeArgs implements java.lang.reflect.Type {

    final Class<?> rawType;
    final java.lang.reflect.Type[] typeArgs;

    TypeWithTypeArgs(Class<?> type, java.lang.reflect.Type[] typeArgs) throws TypeException {
        Resolver.verify(type, typeArgs);

        this.rawType = type;
        this.typeArgs = typeArgs;
    }


    @Override
    public int hashCode() {
        return (rawType.hashCode() * 31) ^ Arrays.hashCode(typeArgs);
    }

    @Override
    public boolean equals(Object other) {
        if (other instanceof TypeWithTypeArgs) {
            TypeWithTypeArgs that = (TypeWithTypeArgs) other;
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
}
