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
import org.apache.kafka.common.utils.Utils;

import java.io.Closeable;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

public class DataOutputStreamWritable implements Writable, Closeable {
    protected final DataOutputStream out;

    public DataOutputStreamWritable(DataOutputStream out) {
        this.out = out;
    }

    @Override
    public void writeByte(byte val) {
        try {
            out.writeByte(val);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void writeShort(short val) {
        try {
            out.writeShort(val);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void writeInt(int val) {
        try {
            out.writeInt(val);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void writeLong(long val) {
        try {
            out.writeLong(val);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void writeDouble(double val) {
        try {
            out.writeDouble(val);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void writeByteArray(byte[] arr) {
        try {
            out.write(arr);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void writeUnsignedVarint(int i) {
        try {
            ByteUtils.writeUnsignedVarint(i, out);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void writeByteBuffer(ByteBuffer buf) {
        try {
            if (buf.hasArray()) {
                out.write(buf.array(), buf.position(), buf.limit());
            } else {
                byte[] bytes = Utils.toArray(buf);
                out.write(bytes);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void writeVarint(int i) {
        try {
            ByteUtils.writeVarint(i, out);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void writeVarlong(long i) {
        try {
            ByteUtils.writeVarlong(i, out);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void flush() {
        try {
            out.flush();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() {
        try {
            out.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
