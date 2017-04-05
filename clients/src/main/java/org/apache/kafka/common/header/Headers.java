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
package org.apache.kafka.common.header;

public interface Headers extends Iterable<Header> {
    
    /**
     * Adds a header (key inside), returning if the operation succeeded.
     * If headers is in read-only, will always fail the operation with throwing IllegalStateException.
     */
    Headers add(Header header) throws IllegalStateException;

    /**
     * Removes ALL HEADERS for the given key returning if the operation succeeded.
     * If headers is in read-only, will always fail the operation with throwing IllegalStateException.
     */
    Headers remove(String key) throws IllegalStateException;

    /**
     * Returns JUST ONE (the very last) header for the given key, if present.
     */
    Header lastHeader(String key);

    /**
     * Returns ALL headers for the given key, in the order they were added in, if present.
     * If NO headers are present for the given key an empty iterable is returned.
     */
    Iterable<Header> headers(String key);

    /**
     * Returns ALL headers as an array, in the order they were added in.
     * If NO headers are present an empty array is returned.
     */
    Header[] toArray();

}
