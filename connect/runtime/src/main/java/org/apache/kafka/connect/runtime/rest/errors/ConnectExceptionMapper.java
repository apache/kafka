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
package org.apache.kafka.connect.runtime.rest.errors;

import org.apache.kafka.connect.errors.AlreadyExistsException;
import org.apache.kafka.connect.errors.NotFoundException;
import org.apache.kafka.connect.runtime.rest.entities.ErrorMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;
import javax.ws.rs.ext.ExceptionMapper;

public class ConnectExceptionMapper implements ExceptionMapper<Exception> {
    private static final Logger log = LoggerFactory.getLogger(ConnectExceptionMapper.class);

    @Context
    private UriInfo uriInfo;

    @Override
    public Response toResponse(Exception exception) {
        log.debug("Uncaught exception in REST call to /{}", uriInfo.getPath(), exception);

        if (exception instanceof ConnectRestException) {
            ConnectRestException restException = (ConnectRestException) exception;
            return Response.status(restException.statusCode())
                    .entity(new ErrorMessage(restException.errorCode(), restException.getMessage()))
                    .build();
        }

        if (exception instanceof NotFoundException) {
            return Response.status(Response.Status.NOT_FOUND)
                    .entity(new ErrorMessage(Response.Status.NOT_FOUND.getStatusCode(), exception.getMessage()))
                    .build();
        }

        if (exception instanceof AlreadyExistsException) {
            return Response.status(Response.Status.CONFLICT)
                    .entity(new ErrorMessage(Response.Status.CONFLICT.getStatusCode(), exception.getMessage()))
                    .build();
        }

        if (!log.isDebugEnabled()) {
            log.error("Uncaught exception in REST call to /{}", uriInfo.getPath(), exception);
        }

        final int statusCode;
        if (exception instanceof WebApplicationException) {
            Response.StatusType statusInfo = ((WebApplicationException) exception).getResponse().getStatusInfo();
            statusCode = statusInfo.getStatusCode();
        } else {
            statusCode = Response.Status.INTERNAL_SERVER_ERROR.getStatusCode();
        }
        return Response.status(statusCode)
                .entity(new ErrorMessage(statusCode, exception.getMessage()))
                .build();
    }
}
