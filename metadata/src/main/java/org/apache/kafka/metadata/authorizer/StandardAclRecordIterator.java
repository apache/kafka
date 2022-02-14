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

import org.apache.kafka.server.common.ApiMessageAndVersion;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;


public class StandardAclRecordIterator implements Iterator<List<ApiMessageAndVersion>> {
    private static final int DEFAULT_MAX_RECORDS_IN_BATCH = 10;

    private final Iterator<StandardAclWithId> iterator;

    private final int maxRecordsInBatch;

    public StandardAclRecordIterator(Iterator<StandardAclWithId> iterator) {
        this(iterator, DEFAULT_MAX_RECORDS_IN_BATCH);
    }

    public StandardAclRecordIterator(Iterator<StandardAclWithId> iterator, int maxRecordsInBatch) {
        this.iterator = iterator;
        this.maxRecordsInBatch = maxRecordsInBatch;
    }

    @Override
    public boolean hasNext() {
        return iterator.hasNext();
    }

    @Override
    public List<ApiMessageAndVersion> next() {
        if (!hasNext()) throw new NoSuchElementException();
        List<ApiMessageAndVersion> result = new ArrayList<>(10);
        for (int i = 0; i < maxRecordsInBatch; i++) {
            if (!iterator.hasNext()) break;
            StandardAclWithId aclWithId = iterator.next();
            result.add(new ApiMessageAndVersion(aclWithId.toRecord(), (short) 0));
        }
        return result;
    }
}
