/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to You under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package org.apache.kafka.common.network;

import java.io.IOException;
import java.nio.channels.GatheringByteChannel;

/**
 *
 * This interface models the in-progress sending of data to a destination identified by an integer id.
 */
public interface Send {

    /**
     * The numeric id for the destination of this send
     * 数字ID为这个发送的目的地
     */
    public String destination();

    /**
     * 是否发送完成
     * Is this send complete?
     */
    public boolean completed();

    /**
     * 写入一些至今没写入的字节根据提供的通道 发送可能需要多次调用才能完全写入
     * Write some as-yet unwritten bytes from this send to the provided channel. It may take multiple calls for the send
     * to be completely written
     * @param channel The Channel to write to
     * @return The number of bytes written
     * @throws IOException If the write fails
     */
    public long writeTo(GatheringByteChannel channel) throws IOException;

    /**
     * Size of the send
     */
    public long size();

}
