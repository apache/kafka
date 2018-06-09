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

import static org.junit.Assert.assertEquals;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.exc.InvalidTypeIdException;
import javax.ws.rs.NotFoundException;
import javax.ws.rs.core.Response;

import org.apache.kafka.common.errors.InvalidRequestException;
import org.apache.kafka.common.errors.SerializationException;
import org.junit.Test;

public class RestExceptionMapperTest {

    @Test
    public void testToResponseNotFound() {
        RestExceptionMapper mapper = new RestExceptionMapper();
        Response resp = mapper.toResponse(new NotFoundException());
        assertEquals(resp.getStatus(), Response.Status.NOT_FOUND.getStatusCode());
    }

    @Test
    public void testToResponseInvalidTypeIdException() {
        RestExceptionMapper mapper = new RestExceptionMapper();
        JsonParser parser = null;
        JavaType type = null;
        Response resp = mapper.toResponse(InvalidTypeIdException.from(parser, "dummy msg", type, "dummy typeId"));
        assertEquals(resp.getStatus(), Response.Status.NOT_IMPLEMENTED.getStatusCode());
    }

    @Test
    public void testToResponseJsonMappingException() {
        RestExceptionMapper mapper = new RestExceptionMapper();
        JsonParser parser = null;
        Response resp = mapper.toResponse(JsonMappingException.from(parser, "dummy msg"));
        assertEquals(resp.getStatus(), Response.Status.BAD_REQUEST.getStatusCode());
    }

    @Test
    public void testToResponseClassNotFoundException() {
        RestExceptionMapper mapper = new RestExceptionMapper();
        Response resp = mapper.toResponse(new ClassNotFoundException());
        assertEquals(resp.getStatus(), Response.Status.NOT_IMPLEMENTED.getStatusCode());
    }

    @Test
    public void testToResponseSerializationException() {
        RestExceptionMapper mapper = new RestExceptionMapper();
        Response resp = mapper.toResponse(new SerializationException());
        assertEquals(resp.getStatus(), Response.Status.BAD_REQUEST.getStatusCode());
    }

    @Test
    public void testToResponseInvalidRequestException() {
        RestExceptionMapper mapper = new RestExceptionMapper();
        Response resp = mapper.toResponse(new InvalidRequestException("invalid request"));
        assertEquals(resp.getStatus(), Response.Status.BAD_REQUEST.getStatusCode());
    }

    @Test
    public void testToResponseUnknownException() {
        RestExceptionMapper mapper = new RestExceptionMapper();
        Response resp = mapper.toResponse(new Exception("Unkown exception"));
        assertEquals(resp.getStatus(), Response.Status.INTERNAL_SERVER_ERROR.getStatusCode());
    }

    @Test(expected = NotFoundException.class)
    public void testToExceptionNotFoundException() throws Exception {
        RestExceptionMapper.toException(Response.Status.NOT_FOUND.getStatusCode(), "Not Found");
    }

    @Test(expected = ClassNotFoundException.class)
    public void testToExceptionClassNotFoundException() throws Exception {
        RestExceptionMapper.toException(Response.Status.NOT_IMPLEMENTED.getStatusCode(), "Not Implemented");
    }

    @Test(expected = InvalidRequestException.class)
    public void testToExceptionSerializationException() throws Exception {
        RestExceptionMapper.toException(Response.Status.BAD_REQUEST.getStatusCode(), "Bad Request");
    }

    @Test(expected = RuntimeException.class)
    public void testToExceptionRuntimeException() throws Exception {
        RestExceptionMapper.toException(-1, "Unkown status code");
    }
}
