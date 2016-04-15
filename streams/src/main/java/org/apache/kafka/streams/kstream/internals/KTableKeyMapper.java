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
 */

package org.apache.kafka.streams.kstream.internals;


/**
 * The KTableKeyMapper interface is for mapping an original key type to a new key type when
 * converting a {@link org.apache.kafka.streams.kstream.KTable} to a {@link org.apache.kafka.streams.kstream.KStream}
 *
 * @param <K1> original key type
 * @param <K2> mapped key type
 */
public interface KTableKeyMapper<K1, K2> {

    K2 apply(K1 key);
}
