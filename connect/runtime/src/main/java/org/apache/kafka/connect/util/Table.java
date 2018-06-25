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
package org.apache.kafka.connect.util;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class Table<R, C, V> {

    private Map<R, Map<C, V>> table = new HashMap<>();

    public V put(R row, C column, V value) {
        Map<C, V> columns = table.get(row);
        if (columns == null) {
            columns = new HashMap<>();
            table.put(row, columns);
        }
        return columns.put(column, value);
    }

    public V get(R row, C column) {
        Map<C, V> columns = table.get(row);
        if (columns == null)
            return null;
        return columns.get(column);
    }

    public Map<C, V> remove(R row) {
        return table.remove(row);
    }

    public V remove(R row, C column) {
        Map<C, V> columns = table.get(row);
        if (columns == null)
            return null;

        V value = columns.remove(column);
        if (columns.isEmpty())
            table.remove(row);
        return value;
    }

    public Map<C, V> row(R row) {
        Map<C, V> columns = table.get(row);
        if (columns == null)
            return Collections.emptyMap();
        return Collections.unmodifiableMap(columns);
    }

}
