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

package org.apache.kafka.metadata.authorizer;


/**
 * An interface used by PrefixNode#walk.
 */
interface PrefixNodeVisitor {
    /**
     * The name that this visitor is interested in. For example, if this is "foobar",
     * we may visit the nodes for "" (empty string), "foo", "foob", and "foobar".
     * But not "bar" or "baz".
     */
    String name();

    /**
     * Process a PrefixNode.
     *
     * @param prefix        The prefix of the PrefixNode. May be the empty string.
     * @param resourceAcls  The resourceAcls contained by the PrefixNode.
     * @return              True to keep walking the tree; false otherwise.
     */
    boolean visit(
        String prefix,
        ResourceAcls resourceAcls
    );
}