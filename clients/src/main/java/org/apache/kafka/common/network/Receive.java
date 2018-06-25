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
package org.apache.kafka.common.network;

import java.io.Closeable;
import java.io.IOException;
import java.nio.channels.ScatteringByteChannel;

/**
 * This interface models the in-progress reading of data from a channel to a source identified by an integer id
 */
public interface Receive extends Closeable {

    /**
     * The numeric id of the source from which we are receiving data.
     */
    String source();

    /**
     * Are we done receiving data?
     */
    boolean complete();

    /**
     * Read bytes into this receive from the given channel
     * @param channel The channel to read from
     * @return The number of bytes read
     * @throws IOException If the reading fails
     */
    long readFrom(ScatteringByteChannel channel) throws IOException;

    /**
     * Do we know yet how much memory we require to fully read this
     */
    boolean requiredMemoryAmountKnown();

    /**
     * Has the underlying memory required to complete reading been allocated yet?
     */
    boolean memoryAllocated();
}
