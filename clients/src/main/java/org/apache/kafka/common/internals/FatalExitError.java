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
package org.apache.kafka.common.internals;

import org.apache.kafka.common.utils.Exit;

/**
 * An error that indicates the need to exit the JVM process. This should only be used by the server or command-line
 * tools. Clients should never shutdown the JVM process.
 *
 * This exception is expected to be caught at the highest level of the thread so that no shared lock is held by
 * the thread when it calls {@link Exit#exit(int)}.
 */
public class FatalExitError extends Error {

    private final static long serialVersionUID = 1L;

    private final int statusCode;

    public FatalExitError(int statusCode) {
        if (statusCode == 0)
            throw new IllegalArgumentException("statusCode must not be 0");
        this.statusCode = statusCode;
    }

    public FatalExitError() {
        this(1);
    }

    public int statusCode() {
        return statusCode;
    }
}
