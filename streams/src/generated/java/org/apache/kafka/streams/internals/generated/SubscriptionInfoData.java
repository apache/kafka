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

package org.apache.kafka.streams.internals.generated;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.TreeMap;
import java.util.UUID;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.protocol.Message;
import org.apache.kafka.common.protocol.MessageUtil;
import org.apache.kafka.common.protocol.ObjectSerializationCache;
import org.apache.kafka.common.protocol.Readable;
import org.apache.kafka.common.protocol.Writable;
import org.apache.kafka.common.protocol.types.ArrayOf;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.protocol.types.RawTaggedField;
import org.apache.kafka.common.protocol.types.RawTaggedFieldWriter;
import org.apache.kafka.common.protocol.types.Schema;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.protocol.types.Type;
import org.apache.kafka.common.utils.ByteUtils;
import org.apache.kafka.common.utils.Bytes;


public class SubscriptionInfoData implements ApiMessage {
    private int version;
    private int latestSupportedVersion;
    private UUID processId;
    private List<TaskId> prevTasks;
    private List<TaskId> standbyTasks;
    private byte[] userEndPoint;
    private List<RawTaggedField> _unknownTaggedFields;
    
    public static final Schema SCHEMA_1 =
        new Schema(
            new Field("version", Type.INT32, ""),
            new Field("process_id", Type.UUID, ""),
            new Field("prev_tasks", new ArrayOf(TaskId.SCHEMA_1), ""),
            new Field("standby_tasks", new ArrayOf(TaskId.SCHEMA_1), "")
        );
    
    public static final Schema SCHEMA_2 =
        new Schema(
            new Field("version", Type.INT32, ""),
            new Field("process_id", Type.UUID, ""),
            new Field("prev_tasks", new ArrayOf(TaskId.SCHEMA_1), ""),
            new Field("standby_tasks", new ArrayOf(TaskId.SCHEMA_1), ""),
            new Field("user_end_point", Type.BYTES, "")
        );
    
    public static final Schema SCHEMA_3 =
        new Schema(
            new Field("version", Type.INT32, ""),
            new Field("latest_supported_version", Type.INT32, ""),
            new Field("process_id", Type.UUID, ""),
            new Field("prev_tasks", new ArrayOf(TaskId.SCHEMA_1), ""),
            new Field("standby_tasks", new ArrayOf(TaskId.SCHEMA_1), ""),
            new Field("user_end_point", Type.BYTES, "")
        );
    
    public static final Schema SCHEMA_4 = SCHEMA_3;
    
    public static final Schema SCHEMA_5 = SCHEMA_4;
    
    public static final Schema[] SCHEMAS = new Schema[] {
        null,
        SCHEMA_1,
        SCHEMA_2,
        SCHEMA_3,
        SCHEMA_4,
        SCHEMA_5
    };
    
    public SubscriptionInfoData(Readable _readable, short _version) {
        read(_readable, _version);
    }
    
    public SubscriptionInfoData(Struct struct, short _version) {
        fromStruct(struct, _version);
    }
    
    public SubscriptionInfoData() {
        this.version = 0;
        this.latestSupportedVersion = -1;
        this.processId = MessageUtil.ZERO_UUID;
        this.prevTasks = new ArrayList<TaskId>();
        this.standbyTasks = new ArrayList<TaskId>();
        this.userEndPoint = Bytes.EMPTY;
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
        return 5;
    }
    
    @Override
    public void read(Readable _readable, short _version) {
        this.version = _readable.readInt();
        if (_version >= 3) {
            this.latestSupportedVersion = _readable.readInt();
        } else {
            this.latestSupportedVersion = -1;
        }
        this.processId = _readable.readUUID();
        {
            int arrayLength;
            arrayLength = _readable.readInt();
            if (arrayLength < 0) {
                throw new RuntimeException("non-nullable field prevTasks was serialized as null");
            } else {
                ArrayList<TaskId> newCollection = new ArrayList<TaskId>(arrayLength);
                for (int i = 0; i < arrayLength; i++) {
                    newCollection.add(new TaskId(_readable, _version));
                }
                this.prevTasks = newCollection;
            }
        }
        {
            int arrayLength;
            arrayLength = _readable.readInt();
            if (arrayLength < 0) {
                throw new RuntimeException("non-nullable field standbyTasks was serialized as null");
            } else {
                ArrayList<TaskId> newCollection = new ArrayList<TaskId>(arrayLength);
                for (int i = 0; i < arrayLength; i++) {
                    newCollection.add(new TaskId(_readable, _version));
                }
                this.standbyTasks = newCollection;
            }
        }
        if (_version >= 2) {
            int length;
            length = _readable.readInt();
            if (length < 0) {
                throw new RuntimeException("non-nullable field userEndPoint was serialized as null");
            } else {
                byte[] newBytes = new byte[length];
                _readable.readArray(newBytes);
                this.userEndPoint = newBytes;
            }
        } else {
            this.userEndPoint = Bytes.EMPTY;
        }
        this._unknownTaggedFields = null;
    }
    
    @Override
    public void write(Writable _writable, ObjectSerializationCache _cache, short _version) {
        int _numTaggedFields = 0;
        _writable.writeInt(version);
        if (_version >= 3) {
            _writable.writeInt(latestSupportedVersion);
        } else {
            if (latestSupportedVersion != -1) {
                throw new UnsupportedVersionException("Attempted to write a non-default latestSupportedVersion at version " + _version);
            }
        }
        _writable.writeUUID(processId);
        _writable.writeInt(prevTasks.size());
        for (TaskId prevTasksElement : prevTasks) {
            prevTasksElement.write(_writable, _cache, _version);
        }
        _writable.writeInt(standbyTasks.size());
        for (TaskId standbyTasksElement : standbyTasks) {
            standbyTasksElement.write(_writable, _cache, _version);
        }
        if (_version >= 2) {
            _writable.writeInt(userEndPoint.length);
            _writable.writeByteArray(userEndPoint);
        } else {
            if (userEndPoint.length != 0) {
                throw new UnsupportedVersionException("Attempted to write a non-default userEndPoint at version " + _version);
            }
        }
        RawTaggedFieldWriter _rawWriter = RawTaggedFieldWriter.forFields(_unknownTaggedFields);
        _numTaggedFields += _rawWriter.numFields();
        if (_numTaggedFields > 0) {
            throw new UnsupportedVersionException("Tagged fields were set, but version " + _version + " of this message does not support them.");
        }
    }
    
    @SuppressWarnings("unchecked")
    @Override
    public void fromStruct(Struct struct, short _version) {
        this._unknownTaggedFields = null;
        this.version = struct.getInt("version");
        if (_version >= 3) {
            this.latestSupportedVersion = struct.getInt("latest_supported_version");
        } else {
            this.latestSupportedVersion = -1;
        }
        this.processId = struct.getUUID("process_id");
        {
            Object[] _nestedObjects = struct.getArray("prev_tasks");
            this.prevTasks = new ArrayList<TaskId>(_nestedObjects.length);
            for (Object nestedObject : _nestedObjects) {
                this.prevTasks.add(new TaskId((Struct) nestedObject, _version));
            }
        }
        {
            Object[] _nestedObjects = struct.getArray("standby_tasks");
            this.standbyTasks = new ArrayList<TaskId>(_nestedObjects.length);
            for (Object nestedObject : _nestedObjects) {
                this.standbyTasks.add(new TaskId((Struct) nestedObject, _version));
            }
        }
        if (_version >= 2) {
            this.userEndPoint = struct.getByteArray("user_end_point");
        } else {
            this.userEndPoint = Bytes.EMPTY;
        }
    }
    
    @Override
    public Struct toStruct(short _version) {
        TreeMap<Integer, Object> _taggedFields = null;
        Struct struct = new Struct(SCHEMAS[_version]);
        struct.set("version", this.version);
        if (_version >= 3) {
            struct.set("latest_supported_version", this.latestSupportedVersion);
        }
        struct.set("process_id", this.processId);
        {
            Struct[] _nestedObjects = new Struct[prevTasks.size()];
            int i = 0;
            for (TaskId element : this.prevTasks) {
                _nestedObjects[i++] = element.toStruct(_version);
            }
            struct.set("prev_tasks", (Object[]) _nestedObjects);
        }
        {
            Struct[] _nestedObjects = new Struct[standbyTasks.size()];
            int i = 0;
            for (TaskId element : this.standbyTasks) {
                _nestedObjects[i++] = element.toStruct(_version);
            }
            struct.set("standby_tasks", (Object[]) _nestedObjects);
        }
        if (_version >= 2) {
            struct.setByteArray("user_end_point", this.userEndPoint);
        }
        return struct;
    }
    
    @Override
    public int size(ObjectSerializationCache _cache, short _version) {
        int _size = 0, _numTaggedFields = 0;
        _size += 4;
        if (_version >= 3) {
            _size += 4;
        }
        _size += 16;
        {
            int _arraySize = 0;
            _arraySize += 4;
            for (TaskId prevTasksElement : prevTasks) {
                _arraySize += prevTasksElement.size(_cache, _version);
            }
            _size += _arraySize;
        }
        {
            int _arraySize = 0;
            _arraySize += 4;
            for (TaskId standbyTasksElement : standbyTasks) {
                _arraySize += standbyTasksElement.size(_cache, _version);
            }
            _size += _arraySize;
        }
        if (_version >= 2) {
            {
                int _bytesSize = userEndPoint.length;
                _bytesSize += 4;
                _size += _bytesSize;
            }
        }
        if (_unknownTaggedFields != null) {
            _numTaggedFields += _unknownTaggedFields.size();
            for (RawTaggedField _field : _unknownTaggedFields) {
                _size += ByteUtils.sizeOfUnsignedVarint(_field.tag());
                _size += ByteUtils.sizeOfUnsignedVarint(_field.size());
                _size += _field.size();
            }
        }
        if (_numTaggedFields > 0) {
            throw new UnsupportedVersionException("Tagged fields were set, but version " + _version + " of this message does not support them.");
        }
        return _size;
    }
    
    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof SubscriptionInfoData)) return false;
        SubscriptionInfoData other = (SubscriptionInfoData) obj;
        if (version != other.version) return false;
        if (latestSupportedVersion != other.latestSupportedVersion) return false;
        if (!this.processId.equals(other.processId)) return false;
        if (this.prevTasks == null) {
            if (other.prevTasks != null) return false;
        } else {
            if (!this.prevTasks.equals(other.prevTasks)) return false;
        }
        if (this.standbyTasks == null) {
            if (other.standbyTasks != null) return false;
        } else {
            if (!this.standbyTasks.equals(other.standbyTasks)) return false;
        }
        if (!Arrays.equals(this.userEndPoint, other.userEndPoint)) return false;
        return true;
    }
    
    @Override
    public int hashCode() {
        int hashCode = 0;
        hashCode = 31 * hashCode + version;
        hashCode = 31 * hashCode + latestSupportedVersion;
        hashCode = 31 * hashCode + processId.hashCode();
        hashCode = 31 * hashCode + (prevTasks == null ? 0 : prevTasks.hashCode());
        hashCode = 31 * hashCode + (standbyTasks == null ? 0 : standbyTasks.hashCode());
        hashCode = 31 * hashCode + Arrays.hashCode(userEndPoint);
        return hashCode;
    }
    
    @Override
    public String toString() {
        return "SubscriptionInfoData("
            + "version=" + version
            + ", latestSupportedVersion=" + latestSupportedVersion
            + ", prevTasks=" + MessageUtil.deepToString(prevTasks.iterator())
            + ", standbyTasks=" + MessageUtil.deepToString(standbyTasks.iterator())
            + ", userEndPoint=" + Arrays.toString(userEndPoint)
            + ")";
    }
    
    public int version() {
        return this.version;
    }
    
    public int latestSupportedVersion() {
        return this.latestSupportedVersion;
    }
    
    public UUID processId() {
        return this.processId;
    }
    
    public List<TaskId> prevTasks() {
        return this.prevTasks;
    }
    
    public List<TaskId> standbyTasks() {
        return this.standbyTasks;
    }
    
    public byte[] userEndPoint() {
        return this.userEndPoint;
    }
    
    @Override
    public List<RawTaggedField> unknownTaggedFields() {
        if (_unknownTaggedFields == null) {
            _unknownTaggedFields = new ArrayList<>(0);
        }
        return _unknownTaggedFields;
    }
    
    public SubscriptionInfoData setVersion(int v) {
        this.version = v;
        return this;
    }
    
    public SubscriptionInfoData setLatestSupportedVersion(int v) {
        this.latestSupportedVersion = v;
        return this;
    }
    
    public SubscriptionInfoData setProcessId(UUID v) {
        this.processId = v;
        return this;
    }
    
    public SubscriptionInfoData setPrevTasks(List<TaskId> v) {
        this.prevTasks = v;
        return this;
    }
    
    public SubscriptionInfoData setStandbyTasks(List<TaskId> v) {
        this.standbyTasks = v;
        return this;
    }
    
    public SubscriptionInfoData setUserEndPoint(byte[] v) {
        this.userEndPoint = v;
        return this;
    }
    
    static public class TaskId implements Message {
        private int topicGroupId;
        private int partition;
        private List<RawTaggedField> _unknownTaggedFields;
        
        public static final Schema SCHEMA_1 =
            new Schema(
                new Field("topic_group_id", Type.INT32, ""),
                new Field("partition", Type.INT32, "")
            );
        
        public static final Schema SCHEMA_2 = SCHEMA_1;
        
        public static final Schema SCHEMA_3 = SCHEMA_2;
        
        public static final Schema SCHEMA_4 = SCHEMA_3;
        
        public static final Schema SCHEMA_5 = SCHEMA_4;
        
        public static final Schema[] SCHEMAS = new Schema[] {
            null,
            SCHEMA_1,
            SCHEMA_2,
            SCHEMA_3,
            SCHEMA_4,
            SCHEMA_5
        };
        
        public TaskId(Readable _readable, short _version) {
            read(_readable, _version);
        }
        
        public TaskId(Struct struct, short _version) {
            fromStruct(struct, _version);
        }
        
        public TaskId() {
            this.topicGroupId = 0;
            this.partition = 0;
        }
        
        
        @Override
        public short lowestSupportedVersion() {
            return 1;
        }
        
        @Override
        public short highestSupportedVersion() {
            return 32767;
        }
        
        @Override
        public void read(Readable _readable, short _version) {
            this.topicGroupId = _readable.readInt();
            this.partition = _readable.readInt();
            this._unknownTaggedFields = null;
        }
        
        @Override
        public void write(Writable _writable, ObjectSerializationCache _cache, short _version) {
            int _numTaggedFields = 0;
            _writable.writeInt(topicGroupId);
            _writable.writeInt(partition);
            RawTaggedFieldWriter _rawWriter = RawTaggedFieldWriter.forFields(_unknownTaggedFields);
            _numTaggedFields += _rawWriter.numFields();
            if (_numTaggedFields > 0) {
                throw new UnsupportedVersionException("Tagged fields were set, but version " + _version + " of this message does not support them.");
            }
        }
        
        @SuppressWarnings("unchecked")
        @Override
        public void fromStruct(Struct struct, short _version) {
            this._unknownTaggedFields = null;
            this.topicGroupId = struct.getInt("topic_group_id");
            this.partition = struct.getInt("partition");
        }
        
        @Override
        public Struct toStruct(short _version) {
            TreeMap<Integer, Object> _taggedFields = null;
            Struct struct = new Struct(SCHEMAS[_version]);
            struct.set("topic_group_id", this.topicGroupId);
            struct.set("partition", this.partition);
            return struct;
        }
        
        @Override
        public int size(ObjectSerializationCache _cache, short _version) {
            int _size = 0, _numTaggedFields = 0;
            _size += 4;
            _size += 4;
            if (_unknownTaggedFields != null) {
                _numTaggedFields += _unknownTaggedFields.size();
                for (RawTaggedField _field : _unknownTaggedFields) {
                    _size += ByteUtils.sizeOfUnsignedVarint(_field.tag());
                    _size += ByteUtils.sizeOfUnsignedVarint(_field.size());
                    _size += _field.size();
                }
            }
            if (_numTaggedFields > 0) {
                throw new UnsupportedVersionException("Tagged fields were set, but version " + _version + " of this message does not support them.");
            }
            return _size;
        }
        
        @Override
        public boolean equals(Object obj) {
            if (!(obj instanceof TaskId)) return false;
            TaskId other = (TaskId) obj;
            if (topicGroupId != other.topicGroupId) return false;
            if (partition != other.partition) return false;
            return true;
        }
        
        @Override
        public int hashCode() {
            int hashCode = 0;
            hashCode = 31 * hashCode + topicGroupId;
            hashCode = 31 * hashCode + partition;
            return hashCode;
        }
        
        @Override
        public String toString() {
            return "TaskId("
                + "topicGroupId=" + topicGroupId
                + ", partition=" + partition
                + ")";
        }
        
        public int topicGroupId() {
            return this.topicGroupId;
        }
        
        public int partition() {
            return this.partition;
        }
        
        @Override
        public List<RawTaggedField> unknownTaggedFields() {
            if (_unknownTaggedFields == null) {
                _unknownTaggedFields = new ArrayList<>(0);
            }
            return _unknownTaggedFields;
        }
        
        public TaskId setTopicGroupId(int v) {
            this.topicGroupId = v;
            return this;
        }
        
        public TaskId setPartition(int v) {
            this.partition = v;
            return this;
        }
    }
}
