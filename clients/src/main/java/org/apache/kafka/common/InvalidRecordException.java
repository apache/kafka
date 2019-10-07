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
package org.apache.kafka.common;

import org.apache.kafka.common.errors.ApiException;
import org.apache.kafka.common.requests.ProduceResponse;

import java.util.Collections;
import java.util.List;

// TODO: remove errorRecords
public class InvalidRecordException extends ApiException {

    private static final long serialVersionUID = 1;

    // Relative offset of the record that throws this exception
    private final List<ProduceResponse.ErrorRecord> errorRecords;

    public InvalidRecordException(String s) {
        super(s);
        this.errorRecords = Collections.emptyList();
    }

    public InvalidRecordException(String s, List<ProduceResponse.ErrorRecord> errorRecords) {
        super(s);
        this.errorRecords = errorRecords;
    }

    public InvalidRecordException(String message, Throwable cause) {
        super(message, cause);
        this.errorRecords = Collections.emptyList();
    }

    public InvalidRecordException(String message, Throwable cause, List<ProduceResponse.ErrorRecord> errorRecords) {
        super(message, cause);
        this.errorRecords = errorRecords;
    }

    public List<ProduceResponse.ErrorRecord> getErrorRecords() {
        return errorRecords;
    }

}
