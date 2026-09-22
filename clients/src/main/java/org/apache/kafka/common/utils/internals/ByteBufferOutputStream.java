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
package org.apache.kafka.common.utils.internals;

import java.io.OutputStream;
import java.nio.ByteBuffer;

/**
 * An {@link OutputStream} backed by one or more {@link ByteBuffer}s, exposing the written bytes via
 * {@link #buffer()}. The default single-buffer implementation is {@link SingleByteBufferOutputStream}.
 */
public abstract class ByteBufferOutputStream extends OutputStream {

    @Override
    public abstract void write(int b);

    @Override
    public abstract void write(byte[] bytes, int off, int len);

    public abstract void write(ByteBuffer sourceBuffer);

    public abstract ByteBuffer buffer();

    public abstract int position();

    public abstract void position(int position);

    public abstract int remaining();

    /**
     * The capacity of the first internal ByteBuffer used by this class. This is useful in cases where a pooled
     * ByteBuffer was passed via the constructor and it needs to be returned to the pool.
     */
    public abstract int initialCapacity();
}
