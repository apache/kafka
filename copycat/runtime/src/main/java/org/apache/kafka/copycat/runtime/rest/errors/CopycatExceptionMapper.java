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

package org.apache.kafka.copycat.runtime.rest.errors;

import org.apache.kafka.copycat.errors.AlreadyExistsException;
import org.apache.kafka.copycat.errors.CopycatException;
import org.apache.kafka.copycat.errors.NotFoundException;
import org.apache.kafka.copycat.runtime.rest.entities.ErrorMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;

public class CopycatExceptionMapper implements ExceptionMapper<CopycatException> {
    private static final Logger log = LoggerFactory.getLogger(CopycatExceptionMapper.class);

    @Override
    public Response toResponse(CopycatException exception) {
        log.debug("Uncaught exception in REST call: ", exception);

        if (exception instanceof CopycatRestException) {
            CopycatRestException restException = (CopycatRestException) exception;
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

        return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                .entity(new ErrorMessage(Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(), exception.getMessage()))
                .build();
    }
}
