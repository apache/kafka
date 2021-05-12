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

package kafka.internals.generated;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.protocol.MessageSizeAccumulator;
import org.apache.kafka.common.protocol.MessageUtil;
import org.apache.kafka.common.protocol.ObjectSerializationCache;
import org.apache.kafka.common.protocol.Readable;
import org.apache.kafka.common.protocol.Writable;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.protocol.types.RawTaggedField;
import org.apache.kafka.common.protocol.types.RawTaggedFieldWriter;
import org.apache.kafka.common.protocol.types.Schema;
import org.apache.kafka.common.protocol.types.Type;
import org.apache.kafka.common.utils.ByteUtils;


public class OffsetCommitKey implements ApiMessage {
    String group;
    String topic;
    int partition;
    private List<RawTaggedField> _unknownTaggedFields;
    
    public static final Schema SCHEMA_0 =
        new Schema(
            new Field("group", Type.STRING, ""),
            new Field("topic", Type.STRING, ""),
            new Field("partition", Type.INT32, "")
        );
    
    public static final Schema SCHEMA_1 = SCHEMA_0;
    
    public static final Schema[] SCHEMAS = new Schema[] {
        SCHEMA_0,
        SCHEMA_1
    };
    
    public static final short LOWEST_SUPPORTED_VERSION = 0;
    public static final short HIGHEST_SUPPORTED_VERSION = 1;
    
    public OffsetCommitKey(Readable _readable, short _version) {
        read(_readable, _version);
    }
    
    public OffsetCommitKey() {
        this.group = "";
        this.topic = "";
        this.partition = 0;
    }
    
    @Override
    public short apiKey() {
        return -1;
    }
    
    @Override
    public short lowestSupportedVersion() {
        return 0;
    }
    
    @Override
    public short highestSupportedVersion() {
        return 1;
    }
    
    @Override
    public void read(Readable _readable, short _version) {
        {
            int length;
            length = _readable.readShort();
            if (length < 0) {
                throw new RuntimeException("non-nullable field group was serialized as null");
            } else if (length > 0x7fff) {
                throw new RuntimeException("string field group had invalid length " + length);
            } else {
                this.group = _readable.readString(length);
            }
        }
        {
            int length;
            length = _readable.readShort();
            if (length < 0) {
                throw new RuntimeException("non-nullable field topic was serialized as null");
            } else if (length > 0x7fff) {
                throw new RuntimeException("string field topic had invalid length " + length);
            } else {
                this.topic = _readable.readString(length);
            }
        }
        this.partition = _readable.readInt();
        this._unknownTaggedFields = null;
    }
    
    @Override
    public void write(Writable _writable, ObjectSerializationCache _cache, short _version) {
        int _numTaggedFields = 0;
        {
            byte[] _stringBytes = _cache.getSerializedValue(group);
            _writable.writeShort((short) _stringBytes.length);
            _writable.writeByteArray(_stringBytes);
        }
        {
            byte[] _stringBytes = _cache.getSerializedValue(topic);
            _writable.writeShort((short) _stringBytes.length);
            _writable.writeByteArray(_stringBytes);
        }
        _writable.writeInt(partition);
        RawTaggedFieldWriter _rawWriter = RawTaggedFieldWriter.forFields(_unknownTaggedFields);
        _numTaggedFields += _rawWriter.numFields();
        if (_numTaggedFields > 0) {
            throw new UnsupportedVersionException("Tagged fields were set, but version " + _version + " of this message does not support them.");
        }
    }
    
    @Override
    public void addSize(MessageSizeAccumulator _size, ObjectSerializationCache _cache, short _version) {
        int _numTaggedFields = 0;
        {
            byte[] _stringBytes = group.getBytes(StandardCharsets.UTF_8);
            if (_stringBytes.length > 0x7fff) {
                throw new RuntimeException("'group' field is too long to be serialized");
            }
            _cache.cacheSerializedValue(group, _stringBytes);
            _size.addBytes(_stringBytes.length + 2);
        }
        {
            byte[] _stringBytes = topic.getBytes(StandardCharsets.UTF_8);
            if (_stringBytes.length > 0x7fff) {
                throw new RuntimeException("'topic' field is too long to be serialized");
            }
            _cache.cacheSerializedValue(topic, _stringBytes);
            _size.addBytes(_stringBytes.length + 2);
        }
        _size.addBytes(4);
        if (_unknownTaggedFields != null) {
            _numTaggedFields += _unknownTaggedFields.size();
            for (RawTaggedField _field : _unknownTaggedFields) {
                _size.addBytes(ByteUtils.sizeOfUnsignedVarint(_field.tag()));
                _size.addBytes(ByteUtils.sizeOfUnsignedVarint(_field.size()));
                _size.addBytes(_field.size());
            }
        }
        if (_numTaggedFields > 0) {
            throw new UnsupportedVersionException("Tagged fields were set, but version " + _version + " of this message does not support them.");
        }
    }
    
    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof OffsetCommitKey)) return false;
        OffsetCommitKey other = (OffsetCommitKey) obj;
        if (this.group == null) {
            if (other.group != null) return false;
        } else {
            if (!this.group.equals(other.group)) return false;
        }
        if (this.topic == null) {
            if (other.topic != null) return false;
        } else {
            if (!this.topic.equals(other.topic)) return false;
        }
        if (partition != other.partition) return false;
        return MessageUtil.compareRawTaggedFields(_unknownTaggedFields, other._unknownTaggedFields);
    }
    
    @Override
    public int hashCode() {
        int hashCode = 0;
        hashCode = 31 * hashCode + (group == null ? 0 : group.hashCode());
        hashCode = 31 * hashCode + (topic == null ? 0 : topic.hashCode());
        hashCode = 31 * hashCode + partition;
        return hashCode;
    }
    
    @Override
    public OffsetCommitKey duplicate() {
        OffsetCommitKey _duplicate = new OffsetCommitKey();
        _duplicate.group = group;
        _duplicate.topic = topic;
        _duplicate.partition = partition;
        return _duplicate;
    }
    
    @Override
    public String toString() {
        return "OffsetCommitKey("
            + "group=" + ((group == null) ? "null" : "'" + group.toString() + "'")
            + ", topic=" + ((topic == null) ? "null" : "'" + topic.toString() + "'")
            + ", partition=" + partition
            + ")";
    }
    
    public String group() {
        return this.group;
    }
    
    public String topic() {
        return this.topic;
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
    
    public OffsetCommitKey setGroup(String v) {
        this.group = v;
        return this;
    }
    
    public OffsetCommitKey setTopic(String v) {
        this.topic = v;
        return this;
    }
    
    public OffsetCommitKey setPartition(int v) {
        this.partition = v;
        return this;
    }
}
