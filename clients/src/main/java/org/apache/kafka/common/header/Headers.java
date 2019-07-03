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
     * Adds a header (key inside), to the end, returning if the operation succeeded.
     * 
     * @param header the Header to be added
     * @return this instance of the Headers, once the header is added.
     * @throws IllegalStateException is thrown if headers are in a read-only state.
     */
    Headers add(Header header) throws IllegalStateException;

    /**
     * Creates and adds a header, to the end, returning if the operation succeeded.
     *
     * @param key of the header to be added.
     * @param value of the header to be added.
     * @return this instance of the Headers, once the header is added.
     * @throws IllegalStateException is thrown if headers are in a read-only state.
     */
    Headers add(String key, byte[] value) throws IllegalStateException;

    /**
     * Removes all headers for the given key returning if the operation succeeded.
     * 
     * @param key to remove all headers for.
     * @return this instance of the Headers, once the header is removed.
     * @throws IllegalStateException is thrown if headers are in a read-only state.
     */
    Headers remove(String key) throws IllegalStateException;

    /**
     * Returns just one (the very last) header for the given key, if present.
     * 
     * @param key to get the last header for.
     * @return this last header matching the given key, returns none if not present.
     */
    Header lastHeader(String key);

    /**
     * Returns all headers for the given key, in the order they were added in, if present.
     *
     * @param key to return the headers for.
     * @return all headers for the given key, in the order they were added in, if NO headers are present an empty iterable is returned. 
     */
    Iterable<Header> headers(String key);

    /**
     * Returns all headers as an array, in the order they were added in.
     *
     * @return the headers as a Header[], mutating this array will not affect the Headers, if NO headers are present an empty array is returned.
     */
    Header[] toArray();

}
