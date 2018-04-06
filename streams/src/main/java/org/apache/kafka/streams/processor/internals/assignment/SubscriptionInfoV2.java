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
package org.apache.kafka.streams.processor.internals.assignment;

import org.apache.kafka.streams.processor.TaskId;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.Set;
import java.util.UUID;

public class SubscriptionInfoV2 extends SubscriptionInfoV1 {
    public SubscriptionInfoV2(UUID processId, Set<TaskId> prevTasks, Set<TaskId> standbyTasks, String userEndPoint) {
        super(processId, prevTasks, standbyTasks, userEndPoint);
    }

    public SubscriptionInfoV2() {

    }

    @Override
    protected int getByteLength(final byte[] endPointBytes) {
        return super.getByteLength(endPointBytes) +
                4 + endPointBytes.length;
    }

    @Override
    public int version() {
        return 2;
    }

    @Override
    protected ByteBuffer doEncode(byte[] endPointBytes) {
        final ByteBuffer buf = ByteBuffer.allocate(getByteLength(endPointBytes));

        buf.putInt(2); // version
        encodeData(buf, endPointBytes);
        return buf;
    }

    @Override
    protected void encodeData(final ByteBuffer buf,
                              final byte[] endPointBytes) {
        super.encodeData(buf, endPointBytes);
        if (endPointBytes != null) {
            buf.putInt(endPointBytes.length);
            buf.put(endPointBytes);
        }
    }

    @Override
    protected void doDecode(
            final ByteBuffer data) {
        super.doDecode(data);

        // decode user end point (can be null)
        int bytesLength = data.getInt();
        if (bytesLength != 0) {
            final byte[] bytes = new byte[bytesLength];
            data.get(bytes);
            userEndPoint = new String(bytes, Charset.forName("UTF-8"));
        }
    }
}
