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

package org.apache.kafka.image.node;

import org.apache.kafka.image.node.printer.MetadataNodePrinter;
import org.apache.kafka.image.node.printer.NodeStringifier;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;


public interface MetadataNode {
    default boolean isDirectory() {
        return true;
    }

    /**
     * Get the names of the children of this node, if there are any.
     */
    default Collection<String> childNames() {
        return Collections.emptyList();
    }

    /**
     * Get the child associated with the given name, or null if there is none.
     */
    default MetadataNode child(String name) {
        return null;
    }

    /**
     * Print this node.
     */
    default void print(MetadataNodePrinter printer) {
        ArrayList<String> names = new ArrayList<>(childNames());
        names.sort(String::compareTo);
        for (String name : names) {
            printer.enterNode(name);
            MetadataNode child = child(name);
            child.print(printer);
            printer.leaveNode();
        }
    }

    /**
     * Convert this node to a string using the default stringifier.
     */
    default String stringify() {
        NodeStringifier stringifier = new NodeStringifier();
        print(stringifier);
        return stringifier.toString();
    }
}
