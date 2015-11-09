/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package org.apache.kafka.connect.runtime.rest.errors;

import org.apache.kafka.connect.errors.ConnectException;

import javax.ws.rs.core.Response;

public class ConnectRestException extends ConnectException {
    private final int statusCode;
    private final int errorCode;

    public ConnectRestException(int statusCode, int errorCode, String message, Throwable t) {
        super(message, t);
        this.statusCode = statusCode;
        this.errorCode = errorCode;
    }

    public ConnectRestException(Response.Status status, int errorCode, String message, Throwable t) {
        this(status.getStatusCode(), errorCode, message, t);
    }

    public ConnectRestException(int statusCode, int errorCode, String message) {
        this(statusCode, errorCode, message, null);
    }

    public ConnectRestException(Response.Status status, int errorCode, String message) {
        this(status, errorCode, message, null);
    }

    public ConnectRestException(int statusCode, String message, Throwable t) {
        this(statusCode, statusCode, message, t);
    }

    public ConnectRestException(Response.Status status, String message, Throwable t) {
        this(status, status.getStatusCode(), message, t);
    }

    public ConnectRestException(int statusCode, String message) {
        this(statusCode, statusCode, message, null);
    }

    public ConnectRestException(Response.Status status, String message) {
        this(status.getStatusCode(), status.getStatusCode(), message, null);
    }


    public int statusCode() {
        return statusCode;
    }

    public int errorCode() {
        return errorCode;
    }
}
