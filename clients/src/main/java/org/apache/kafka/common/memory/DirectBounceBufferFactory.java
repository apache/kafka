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

package org.apache.kafka.common.memory;

import org.apache.kafka.common.utils.ByteBufferUnmapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;

public class DirectBounceBufferFactory implements BounceBufferFactory {
    private static final Logger log = LoggerFactory.getLogger(DirectBounceBufferFactory.class);

    @Override
    public ByteBuffer allocate(int maxSize) {
        return ByteBuffer.allocateDirect(maxSize);
    }

    @Override
    public void free(ByteBuffer buffer) {
        try {
            ByteBufferUnmapper.unmap("BounceBuffer", buffer);
        } catch (Throwable e) {
            log.warn("Unable to unmap DirectByteBuffer", e);
        }
    }
}
