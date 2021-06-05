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
package org.apache.kafka.connect.storage;

import java.io.Closeable;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.Future;

public interface CloseableOffsetStorageReader extends Closeable, OffsetStorageReader {

    /**
     * {@link Future#cancel(boolean) Cancel} all outstanding offset read requests, and throw an
     * exception in all current and future calls to {@link #offsets(Collection)} and
     * {@link #offset(Map)}. This is useful for unblocking task threads which need to shut down but
     * are blocked on offset reads.
     */
    void close();
}
