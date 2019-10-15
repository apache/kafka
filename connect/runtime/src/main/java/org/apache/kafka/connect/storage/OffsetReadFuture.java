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
package org.apache.kafka.connect.storage;


import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.Future;

public interface OffsetReadFuture extends Future<Map<ByteBuffer, ByteBuffer>> {

    /**
     * Without {@link Future#cancel(boolean) cancelling} the operation, attempt to immediately
     * unblock any threads which are waiting for this future to complete, providing them with
     * potentially incomplete or out-of-date offset information if necessary.
     */
    void prematurelyComplete();
}