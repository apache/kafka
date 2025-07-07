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

package org.apache.kafka.common.test;


import org.apache.kafka.server.fault.FaultHandler;
import org.apache.kafka.server.fault.FaultHandlerException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is a fault handler suitable for use in JUnit tests. It will store the result of the first
 * call to handleFault that was made.
 */
public class MockFaultHandler implements FaultHandler {
    private static final Logger log = LoggerFactory.getLogger(MockFaultHandler.class);

    private final String name;
    private FaultHandlerException firstException = null;
    private boolean ignore = false;

    public MockFaultHandler(String name) {
        this.name = name;
    }

    @Override
    public synchronized RuntimeException handleFault(String failureMessage, Throwable cause) {
        if (cause == null) {
            log.error("Encountered {} fault: {}", name, failureMessage);
        } else {
            log.error("Encountered {} fault: {}", name, failureMessage, cause);
        }
        FaultHandlerException e = (cause == null) ?
                new FaultHandlerException(name + ": " + failureMessage) :
                new FaultHandlerException(name + ": " + failureMessage +
                        ": " + cause.getMessage(), cause);
        if (firstException == null) {
            firstException = e;
        }
        return firstException;
    }

    public synchronized void maybeRethrowFirstException() {
        if (firstException != null && !ignore) {
            throw firstException;
        }
    }

    public synchronized FaultHandlerException firstException() {
        return firstException;
    }

    public synchronized void setIgnore(boolean ignore) {
        this.ignore = ignore;
    }
}
