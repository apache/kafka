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
package org.apache.kafka.common.protocol;

import org.apache.kafka.common.utils.ByteUtils;

import java.io.DataInput;
import java.io.IOException;
import java.nio.ByteBuffer;

public class DataInputReadable implements Readable {
    private final DataInput input;

    public DataInputReadable(DataInput input) {
        this.input = input;
    }

    @Override
    public byte readByte() {
        try {
            return input.readByte();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public short readShort() {
        try {
            return input.readShort();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public int readInt() {
        try {
            return input.readInt();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public long readLong() {
        try {
            return input.readLong();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public double readDouble() {
        try {
            return input.readDouble();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void readArray(byte[] arr) {
        try {
            input.readFully(arr);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public int readUnsignedVarint() {
        try {
            return ByteUtils.readUnsignedVarint(input);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public ByteBuffer readByteBuffer(int length) {
        // TODO: We could try to be clever here and detect when the underlying
        // data stream supports ByteBuffer slices (as would be the case when
        // compression is not in use)
        byte[] array = new byte[length];
        readArray(array);
        return ByteBuffer.wrap(array);
    }

    @Override
    public int readVarint() {
        try {
            return ByteUtils.readVarint(input);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public long readVarlong() {
        try {
            return ByteUtils.readVarlong(input);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

}
