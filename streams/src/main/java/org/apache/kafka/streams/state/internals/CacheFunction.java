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

package org.apache.kafka.streams.state.internals;

import org.apache.kafka.common.utils.Bytes;

import java.nio.ByteBuffer;

interface CacheFunction {
    // return ByteBuffer here since we usually do further content extraction (ex: extractKeyBytes) with the key.
    // With ByteBuffer, we can better manipulate this partial content extraction without array copy
    ByteBuffer key(Bytes cacheKey);
    // return Bytes here since we won't have further operation to this cacheKey
    Bytes cacheKey(Bytes cacheKey);
}
