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
package org.apache.kafka.streams.processor.internals;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.NoSuchElementException;

public class QuickUnion<T> {

    private HashMap<T, T> ids = new HashMap<>();

    public void add(T id) {
        ids.put(id, id);
    }

    public boolean exists(T id) {
        return ids.containsKey(id);
    }

    /**
     * @throws NoSuchElementException if this node or the parent of this node is null
     */
    public T root(T id) {
        T current = id;
        if (current == null) {
            throw new NoSuchElementException("id: " + id.toString());
        }
        T parent = ids.get(current);
        if (parent == null) {
            throw new NoSuchElementException("id: " + id.toString());
        }
        List<T> subNodes = new LinkedList<>();
        while (!current.equals(parent)) {
            subNodes.add(current);
            current = parent;
            if (current == null) {
                throw new NoSuchElementException("id: " + id.toString());
            }
            parent = ids.get(current);
        }
        for (T node : subNodes) {
            ids.put(node, current);
        }
        return current;
    }

    @SuppressWarnings("unchecked")
    public void unite(T id1, T... idList) {
        for (T id2 : idList) {
            unitePair(id1, id2);
        }
    }

    private void unitePair(T id1, T id2) {
        T root1 = root(id1);
        T root2 = root(id2);

        if (!root1.equals(root2))
            ids.put(root1, root2);
    }

}
