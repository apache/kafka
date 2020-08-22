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

package org.apache.kafka.common.protocol.types;

import java.util.Arrays;

public class RawTaggedField {
    private final int tag;
    private final byte[] data;

    public RawTaggedField(int tag, byte[] data) {
        this.tag = tag;
        this.data = data;
    }

    public int tag() {
        return tag;
    }

    public byte[] data() {
        return data;
    }

    public int size() {
        return data.length;
    }

    @Override
    public boolean equals(Object o) {
        if ((o == null) || (!o.getClass().equals(getClass()))) {
            return false;
        }
        RawTaggedField other = (RawTaggedField) o;
        return tag == other.tag && Arrays.equals(data, other.data);
    }

    @Override
    public int hashCode() {
        return tag ^ Arrays.hashCode(data);
    }
}
