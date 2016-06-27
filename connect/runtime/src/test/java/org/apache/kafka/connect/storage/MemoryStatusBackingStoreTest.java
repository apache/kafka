/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/
package org.apache.kafka.connect.storage;

import org.apache.kafka.connect.runtime.ConnectorStatus;
import org.apache.kafka.connect.runtime.TaskStatus;
import org.apache.kafka.connect.util.ConnectorTaskId;
import org.junit.Test;

import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class MemoryStatusBackingStoreTest {

    @Test
    public void putAndGetConnectorStatus() {
        MemoryStatusBackingStore store = new MemoryStatusBackingStore();
        ConnectorStatus status = new ConnectorStatus("connector", ConnectorStatus.State.RUNNING, "localhost:8083", 0);
        store.put(status);
        assertEquals(status, store.get("connector"));
    }

    @Test
    public void putAndGetTaskStatus() {
        MemoryStatusBackingStore store = new MemoryStatusBackingStore();
        ConnectorTaskId taskId = new ConnectorTaskId("connector", 0);
        TaskStatus status = new TaskStatus(taskId, ConnectorStatus.State.RUNNING, "localhost:8083", 0);
        store.put(status);
        assertEquals(status, store.get(taskId));
        assertEquals(Collections.singleton(status), store.getAll("connector"));
    }

    @Test
    public void deleteConnectorStatus() {
        MemoryStatusBackingStore store = new MemoryStatusBackingStore();
        store.put(new ConnectorStatus("connector", ConnectorStatus.State.RUNNING, "localhost:8083", 0));
        store.put(new ConnectorStatus("connector", ConnectorStatus.State.DESTROYED, "localhost:8083", 0));
        assertNull(store.get("connector"));
    }

    @Test
    public void deleteTaskStatus() {
        MemoryStatusBackingStore store = new MemoryStatusBackingStore();
        ConnectorTaskId taskId = new ConnectorTaskId("connector", 0);
        store.put(new TaskStatus(taskId, ConnectorStatus.State.RUNNING, "localhost:8083", 0));
        store.put(new TaskStatus(taskId, ConnectorStatus.State.DESTROYED, "localhost:8083", 0));
        assertNull(store.get(taskId));
    }

}
