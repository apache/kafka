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
package org.apache.kafka.raft.internals;

import org.apache.kafka.common.protocol.ObjectSerializationCache;
import org.apache.kafka.common.protocol.Readable;
import org.apache.kafka.common.protocol.Writable;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.server.common.serialization.RecordSerde;

public class StringSerde implements RecordSerde<String> {

    @Override
    public int recordSize(String data, ObjectSerializationCache serializationCache) {
        return recordSize(data);
    }

    public int recordSize(String data) {
        return Utils.utf8Length(data);
    }

    @Override
    public void write(String data, ObjectSerializationCache serializationCache, Writable out) {
        out.writeByteArray(Utils.utf8(data));
    }

    @Override
    public String read(Readable input, int size) {
        byte[] data = new byte[size];
        input.readArray(data);
        return Utils.utf8(data);
    }

}
