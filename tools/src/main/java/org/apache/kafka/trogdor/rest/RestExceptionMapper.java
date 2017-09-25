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
package org.apache.kafka.trogdor.rest;

import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.exc.InvalidTypeIdException;
import org.apache.kafka.common.errors.SerializationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.NotFoundException;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;

public class RestExceptionMapper implements ExceptionMapper<Throwable> {
    private static final Logger log = LoggerFactory.getLogger(RestExceptionMapper.class);

    @Override
    public Response toResponse(Throwable e) {
        if (log.isDebugEnabled()) {
            log.debug("Uncaught exception in REST call: ", e);
        } else if (log.isInfoEnabled()) {
            log.info("Uncaught exception in REST call: {}", e.getMessage());
        }
        if (e instanceof NotFoundException) {
            return buildResponse(Response.Status.NOT_FOUND, e);
        } else if (e instanceof JsonMappingException) {
            return buildResponse(Response.Status.BAD_REQUEST, e);
        } else if (e instanceof ClassNotFoundException) {
            return buildResponse(Response.Status.NOT_IMPLEMENTED, e);
        } else if (e instanceof InvalidTypeIdException) {
            return buildResponse(Response.Status.NOT_IMPLEMENTED, e);
        } else if (e instanceof SerializationException) {
            return buildResponse(Response.Status.BAD_REQUEST, e);
        } else {
            return buildResponse(Response.Status.INTERNAL_SERVER_ERROR, e);
        }
    }

    public static Exception toException(int code, String msg) throws Exception {
        if (code == Response.Status.NOT_FOUND.getStatusCode()) {
            throw new NotFoundException(msg);
        } else if (code == Response.Status.NOT_IMPLEMENTED.getStatusCode()) {
            throw new ClassNotFoundException(msg);
        } else if (code == Response.Status.BAD_REQUEST.getStatusCode()) {
            throw new SerializationException(msg);
        } else {
            throw new RuntimeException(msg);
        }
    }

    private Response buildResponse(Response.Status code, Throwable e) {
        return Response.status(code).
            entity(new ErrorResponse(code.getStatusCode(), e.getMessage())).
            build();
    }
}
