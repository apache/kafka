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

package org.apache.kafka.server;

import org.apache.kafka.common.requests.EnvelopeRequest;
import org.apache.kafka.common.requests.RequestContext;
import org.apache.kafka.common.security.auth.KafkaPrincipalSerde;

import java.nio.ByteBuffer;

public final class ForwardingManagerUtil {
    public static EnvelopeRequest.Builder buildEnvelopeRequest(RequestContext context, ByteBuffer forwardRequestBuffer) {
        KafkaPrincipalSerde principalSerde = context.principalSerde.orElseThrow(() ->
            new IllegalArgumentException(
                "Cannot deserialize principal from request context " + context +
                " since there is no serde defined"
            )
        );
        byte[] serializedPrincipal = principalSerde.serialize(context.principal);
        return new EnvelopeRequest.Builder(
            forwardRequestBuffer,
            serializedPrincipal,
            context.clientAddress.getAddress()
        );
    }
}
