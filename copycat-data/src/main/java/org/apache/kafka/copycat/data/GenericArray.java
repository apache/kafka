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
 **/



package org.apache.kafka.copycat.data;

import java.util.List;

/** Array that permits reuse of contained elements. */
public interface GenericArray<T> extends List<T>, GenericContainer {
    /** The current content of the location where {@link #add(Object)} would next
     * store an element, if any.  This permits reuse of arrays and their elements
     * without allocating new objects. */
    T peek();

    /** Reverses the order of the elements in this array. */
    void reverse();
}

