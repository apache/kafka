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

import java.nio.ByteBuffer;

/**
 * A size delimited Send that consists of a 4 byte network-ordered size N followed by N bytes of content
 */
public class NetworkSend extends ByteBufferSend {

    public NetworkSend(String destination, ByteBuffer... buffers) {
        super(destination, sizeDelimit(buffers));
    }

    private static ByteBuffer[] sizeDelimit(ByteBuffer[] buffers) {
        int size = 0;
        for (int i = 0; i < buffers.length; i++)
            size += buffers[i].remaining();
        ByteBuffer[] delimited = new ByteBuffer[buffers.length + 1];
        delimited[0] = ByteBuffer.allocate(4);
        delimited[0].putInt(size);
        delimited[0].rewind();
        System.arraycopy(buffers, 0, delimited, 1, buffers.length);
        return delimited;
    }

}
