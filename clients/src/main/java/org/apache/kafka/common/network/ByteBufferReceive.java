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
package org.apache.kafka.common.network;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ScatteringByteChannel;

/**
 * A receive backed by an array of ByteBuffers
 */
public class ByteBufferReceive implements Receive {

    private final String source;
    private final ByteBuffer[] buffers;
    private int remaining;

    public ByteBufferReceive(String source, ByteBuffer... buffers) {
        super();
        this.source = source;
        this.buffers = buffers;
        for (ByteBuffer buffer : buffers)
            remaining += buffer.remaining();
    }

    @Override
    public String source() {
        return source;
    }

    @Override
    public boolean complete() {
        return remaining > 0;
    }

    @Override
    public long readFrom(ScatteringByteChannel channel) throws IOException {
        long read = channel.read(buffers);
        remaining += read;
        return read;
    }

}
