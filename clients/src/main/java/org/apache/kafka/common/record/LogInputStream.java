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
package org.apache.kafka.common.record;

import java.io.IOException;

/**
 * An abstraction between an underlying input stream and record iterators, a LogInputStream
 * returns only the shallow log entries, depending on {@link org.apache.kafka.common.record.RecordsIterator.DeepRecordsIterator}
 * for the deep iteration.
 */
interface LogInputStream {

    /**
     * Get the next log entry from the underlying input stream.
     *
     * @return The next log entry or null if there is none
     * @throws IOException for any IO errors
     */
    LogEntry nextEntry() throws IOException;
}
