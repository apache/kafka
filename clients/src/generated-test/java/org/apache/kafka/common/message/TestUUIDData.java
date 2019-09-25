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

// THIS CODE IS AUTOMATICALLY GENERATED.  DO NOT EDIT.

package org.apache.kafka.common.message;

import java.util.UUID;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.protocol.Readable;
import org.apache.kafka.common.protocol.Writable;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.protocol.types.Schema;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.protocol.types.Type;


public class TestUUIDData implements ApiMessage {
    private UUID processId;
    
    public static final Schema SCHEMA_1 =
        new Schema(
            new Field("process_id", Type.UUID, "")
        );
    
    public static final Schema[] SCHEMAS = new Schema[] {
        null,
        SCHEMA_1
    };
    
    public TestUUIDData(Readable readable, short version) {
        read(readable, version);
    }
    
    public TestUUIDData(Struct struct, short version) {
        fromStruct(struct, version);
    }
    
    public TestUUIDData() {
        this.processId = org.apache.kafka.common.protocol.MessageUtil.ZERO_UUID;
    }
    
    @Override
    public short apiKey() {
        return -1;
    }
    
    @Override
    public short lowestSupportedVersion() {
        return 1;
    }
    
    @Override
    public short highestSupportedVersion() {
        return 1;
    }
    
    @Override
    public void read(Readable readable, short version) {
        this.processId = readable.readUUID();
    }
    
    @Override
    public void write(Writable writable, short version) {
        writable.writeUUID(processId);
    }
    
    @Override
    public void fromStruct(Struct struct, short version) {
        this.processId = struct.getUUID("process_id");
    }
    
    @Override
    public Struct toStruct(short version) {
        Struct struct = new Struct(SCHEMAS[version]);
        struct.set("process_id", this.processId);
        return struct;
    }
    
    @Override
    public int size(short version) {
        int size = 0;
        size += 16;
        return size;
    }
    
    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof TestUUIDData)) return false;
        TestUUIDData other = (TestUUIDData) obj;
        if (processId != other.processId) return false;
        return true;
    }
    
    @Override
    public int hashCode() {
        int hashCode = 0;
        hashCode = 31 * hashCode + (processId == null ? 0 : processId.hashCode());
        return hashCode;
    }
    
    @Override
    public String toString() {
        return "TestUUIDData("
            + "processId=" + processId
            + ")";
    }
    
    public UUID processId() {
        return this.processId;
    }
    
    public TestUUIDData setProcessId(UUID v) {
        this.processId = v;
        return this;
    }
}
