/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to You under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package org.apache.kafka.common.errors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The exception that indicates the need to exit the process. This exception is expected to be
 * caught at the highest level of the thread so that no shared lock would be held by the thread
 * when it calls {@link System#exit(int)}
 */
public class FatalExitError extends Error {

    private static final Logger log = LoggerFactory.getLogger(FatalExitError.class);

    private final static long serialVersionUID = 1L;

    protected int exitStatus;

    public FatalExitError(int exitStatus, String message, Throwable cause) {
        super(message, cause);
        this.exitStatus = exitStatus;
    }

    public FatalExitError(int exitStatus, String message) {
        super(message);
        this.exitStatus = exitStatus;
    }

    public FatalExitError(int exitStatus, Throwable cause) {
        super(cause);
        this.exitStatus = exitStatus;
    }

    public FatalExitError(int exitStatus) {
        super();
        this.exitStatus = exitStatus;
    }

    public void shutdownSystem() {
        log.error("System.exit is invoked", this);
        System.exit(exitStatus);
    }
}
