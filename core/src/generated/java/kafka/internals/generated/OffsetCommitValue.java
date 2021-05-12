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


public class OffsetCommitValue implements ApiMessage {
    long offset;
    int leaderEpoch;
    String metadata;
    long commitTimestamp;
    long expireTimestamp;
    private List<RawTaggedField> _unknownTaggedFields;
    
    public static final Schema SCHEMA_0 =
        new Schema(
            new Field("offset", Type.INT64, ""),
            new Field("metadata", Type.STRING, ""),
            new Field("commit_timestamp", Type.INT64, "")
        );
    
    public static final Schema SCHEMA_1 =
        new Schema(
            new Field("offset", Type.INT64, ""),
            new Field("metadata", Type.STRING, ""),
            new Field("commit_timestamp", Type.INT64, ""),
            new Field("expire_timestamp", Type.INT64, "")
        );
    
    public static final Schema SCHEMA_2 =
        new Schema(
            new Field("offset", Type.INT64, ""),
            new Field("metadata", Type.STRING, ""),
            new Field("commit_timestamp", Type.INT64, "")
        );
    
    public static final Schema SCHEMA_3 =
        new Schema(
            new Field("offset", Type.INT64, ""),
            new Field("leader_epoch", Type.INT32, ""),
            new Field("metadata", Type.STRING, ""),
            new Field("commit_timestamp", Type.INT64, "")
        );
    
    public static final Schema[] SCHEMAS = new Schema[] {
        SCHEMA_0,
        SCHEMA_1,
        SCHEMA_2,
        SCHEMA_3
    };
    
    public static final short LOWEST_SUPPORTED_VERSION = 0;
    public static final short HIGHEST_SUPPORTED_VERSION = 3;
    
    public OffsetCommitValue(Readable _readable, short _version) {
        read(_readable, _version);
    }
    
    public OffsetCommitValue() {
        this.offset = 0L;
        this.leaderEpoch = -1;
        this.metadata = "";
        this.commitTimestamp = 0L;
        this.expireTimestamp = -1L;
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
        return 3;
    }
    
    @Override
    public void read(Readable _readable, short _version) {
        this.offset = _readable.readLong();
        if (_version >= 3) {
            this.leaderEpoch = _readable.readInt();
        } else {
            this.leaderEpoch = -1;
        }
        {
            int length;
            length = _readable.readShort();
            if (length < 0) {
                throw new RuntimeException("non-nullable field metadata was serialized as null");
            } else if (length > 0x7fff) {
                throw new RuntimeException("string field metadata had invalid length " + length);
            } else {
                this.metadata = _readable.readString(length);
            }
        }
        this.commitTimestamp = _readable.readLong();
        if ((_version >= 1) && (_version <= 1)) {
            this.expireTimestamp = _readable.readLong();
        } else {
            this.expireTimestamp = -1L;
        }
        this._unknownTaggedFields = null;
    }
    
    @Override
    public void write(Writable _writable, ObjectSerializationCache _cache, short _version) {
        int _numTaggedFields = 0;
        _writable.writeLong(offset);
        if (_version >= 3) {
            _writable.writeInt(leaderEpoch);
        }
        {
            byte[] _stringBytes = _cache.getSerializedValue(metadata);
            _writable.writeShort((short) _stringBytes.length);
            _writable.writeByteArray(_stringBytes);
        }
        _writable.writeLong(commitTimestamp);
        if ((_version >= 1) && (_version <= 1)) {
            _writable.writeLong(expireTimestamp);
        }
        RawTaggedFieldWriter _rawWriter = RawTaggedFieldWriter.forFields(_unknownTaggedFields);
        _numTaggedFields += _rawWriter.numFields();
        if (_numTaggedFields > 0) {
            throw new UnsupportedVersionException("Tagged fields were set, but version " + _version + " of this message does not support them.");
        }
    }
    
    @Override
    public void addSize(MessageSizeAccumulator _size, ObjectSerializationCache _cache, short _version) {
        int _numTaggedFields = 0;
        _size.addBytes(8);
        if (_version >= 3) {
            _size.addBytes(4);
        }
        {
            byte[] _stringBytes = metadata.getBytes(StandardCharsets.UTF_8);
            if (_stringBytes.length > 0x7fff) {
                throw new RuntimeException("'metadata' field is too long to be serialized");
            }
            _cache.cacheSerializedValue(metadata, _stringBytes);
            _size.addBytes(_stringBytes.length + 2);
        }
        _size.addBytes(8);
        if ((_version >= 1) && (_version <= 1)) {
            _size.addBytes(8);
        }
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
        if (!(obj instanceof OffsetCommitValue)) return false;
        OffsetCommitValue other = (OffsetCommitValue) obj;
        if (offset != other.offset) return false;
        if (leaderEpoch != other.leaderEpoch) return false;
        if (this.metadata == null) {
            if (other.metadata != null) return false;
        } else {
            if (!this.metadata.equals(other.metadata)) return false;
        }
        if (commitTimestamp != other.commitTimestamp) return false;
        if (expireTimestamp != other.expireTimestamp) return false;
        return MessageUtil.compareRawTaggedFields(_unknownTaggedFields, other._unknownTaggedFields);
    }
    
    @Override
    public int hashCode() {
        int hashCode = 0;
        hashCode = 31 * hashCode + ((int) (offset >> 32) ^ (int) offset);
        hashCode = 31 * hashCode + leaderEpoch;
        hashCode = 31 * hashCode + (metadata == null ? 0 : metadata.hashCode());
        hashCode = 31 * hashCode + ((int) (commitTimestamp >> 32) ^ (int) commitTimestamp);
        hashCode = 31 * hashCode + ((int) (expireTimestamp >> 32) ^ (int) expireTimestamp);
        return hashCode;
    }
    
    @Override
    public OffsetCommitValue duplicate() {
        OffsetCommitValue _duplicate = new OffsetCommitValue();
        _duplicate.offset = offset;
        _duplicate.leaderEpoch = leaderEpoch;
        _duplicate.metadata = metadata;
        _duplicate.commitTimestamp = commitTimestamp;
        _duplicate.expireTimestamp = expireTimestamp;
        return _duplicate;
    }
    
    @Override
    public String toString() {
        return "OffsetCommitValue("
            + "offset=" + offset
            + ", leaderEpoch=" + leaderEpoch
            + ", metadata=" + ((metadata == null) ? "null" : "'" + metadata.toString() + "'")
            + ", commitTimestamp=" + commitTimestamp
            + ", expireTimestamp=" + expireTimestamp
            + ")";
    }
    
    public long offset() {
        return this.offset;
    }
    
    public int leaderEpoch() {
        return this.leaderEpoch;
    }
    
    public String metadata() {
        return this.metadata;
    }
    
    public long commitTimestamp() {
        return this.commitTimestamp;
    }
    
    public long expireTimestamp() {
        return this.expireTimestamp;
    }
    
    @Override
    public List<RawTaggedField> unknownTaggedFields() {
        if (_unknownTaggedFields == null) {
            _unknownTaggedFields = new ArrayList<>(0);
        }
        return _unknownTaggedFields;
    }
    
    public OffsetCommitValue setOffset(long v) {
        this.offset = v;
        return this;
    }
    
    public OffsetCommitValue setLeaderEpoch(int v) {
        this.leaderEpoch = v;
        return this;
    }
    
    public OffsetCommitValue setMetadata(String v) {
        this.metadata = v;
        return this;
    }
    
    public OffsetCommitValue setCommitTimestamp(long v) {
        this.commitTimestamp = v;
        return this;
    }
    
    public OffsetCommitValue setExpireTimestamp(long v) {
        this.expireTimestamp = v;
        return this;
    }
}
