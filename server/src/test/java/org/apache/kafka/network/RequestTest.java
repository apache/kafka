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

package org.apache.kafka.network;

import org.apache.kafka.clients.admin.AlterConfigOp.OpType;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.config.types.Password;
import org.apache.kafka.common.memory.MemoryPool;
import org.apache.kafka.common.message.CreateTopicsRequestData;
import org.apache.kafka.common.message.CreateTopicsRequestData.CreatableTopic;
import org.apache.kafka.common.message.CreateTopicsResponseData;
import org.apache.kafka.common.message.IncrementalAlterConfigsRequestData;
import org.apache.kafka.common.message.IncrementalAlterConfigsRequestData.AlterConfigsResource;
import org.apache.kafka.common.message.IncrementalAlterConfigsRequestData.AlterableConfig;
import org.apache.kafka.common.message.IncrementalAlterConfigsRequestData.AlterableConfigCollection;
import org.apache.kafka.common.network.ClientInformation;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.network.Send;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.AlterConfigsRequest;
import org.apache.kafka.common.requests.AlterConfigsRequest.Config;
import org.apache.kafka.common.requests.AlterConfigsRequest.ConfigEntry;
import org.apache.kafka.common.requests.ByteBufferChannel;
import org.apache.kafka.common.requests.CreateTopicsRequest;
import org.apache.kafka.common.requests.CreateTopicsResponse;
import org.apache.kafka.common.requests.EnvelopeRequest;
import org.apache.kafka.common.requests.EnvelopeResponse;
import org.apache.kafka.common.requests.IncrementalAlterConfigsRequest;
import org.apache.kafka.common.requests.MetadataRequest;
import org.apache.kafka.common.requests.RequestContext;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.security.auth.KafkaPrincipalSerde;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.common.utils.internals.SecurityUtils;
import org.apache.kafka.network.metrics.RequestChannelMetrics;
import org.apache.kafka.server.EnvelopeUtils;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.kafka.common.config.SaslConfigs.SASL_JAAS_CONFIG;
import static org.apache.kafka.common.config.SaslConfigs.SASL_LOGIN_CALLBACK_HANDLER_CLASS;
import static org.apache.kafka.common.config.SslConfigs.SSL_CIPHER_SUITES_CONFIG;
import static org.apache.kafka.common.config.SslConfigs.SSL_ENABLED_PROTOCOLS_CONFIG;
import static org.apache.kafka.common.config.SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG;
import static org.apache.kafka.common.config.SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG;
import static org.apache.kafka.common.config.SslConfigs.SSL_KEY_PASSWORD_CONFIG;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.mock;

public class RequestTest {

    private static final String SENSITIVE_VALUE = "secret";

    private final RequestChannelMetrics requestChannelMetrics = mock(RequestChannelMetrics.class);
    private final KafkaPrincipalSerde principalSerde = new KafkaPrincipalSerde() {
        @Override
        public byte[] serialize(KafkaPrincipal principal) {
            return Utils.utf8(principal.toString());
        }

        @Override
        public KafkaPrincipal deserialize(byte[] bytes) {
            return SecurityUtils.parseKafkaPrincipal(Utils.utf8(bytes));
        }
    };

    @Test
    public void testAlterRequests() {
        ConfigResource brokerResource = new ConfigResource(ConfigResource.Type.BROKER, "1");
        ConfigEntry keystorePassword = new ConfigEntry(SSL_KEYSTORE_PASSWORD_CONFIG, SENSITIVE_VALUE);
        verifyAlterConfig(brokerResource, List.of(keystorePassword), Map.of(SSL_KEYSTORE_PASSWORD_CONFIG, Password.HIDDEN));

        ConfigEntry keystoreLocation = new ConfigEntry(SSL_KEYSTORE_LOCATION_CONFIG, "/path/to/keystore");
        verifyAlterConfig(brokerResource, List.of(keystoreLocation), Map.of(SSL_KEYSTORE_LOCATION_CONFIG, "/path/to/keystore"));
        verifyAlterConfig(
            brokerResource, 
            List.of(keystoreLocation, keystorePassword), 
            Map.of(
                SSL_KEYSTORE_LOCATION_CONFIG, "/path/to/keystore", 
                SSL_KEYSTORE_PASSWORD_CONFIG, Password.HIDDEN
            )
        );

        ConfigEntry listenerKeyPassword = new ConfigEntry("listener.name.internal." + SSL_KEY_PASSWORD_CONFIG, SENSITIVE_VALUE);
        verifyAlterConfig(brokerResource, List.of(listenerKeyPassword), Map.of(listenerKeyPassword.name(), Password.HIDDEN));

        ConfigEntry listenerKeystore = new ConfigEntry("listener.name.internal." + SSL_KEYSTORE_LOCATION_CONFIG, "/path/to/keystore");
        verifyAlterConfig(brokerResource, List.of(listenerKeystore), Map.of(listenerKeystore.name(), "/path/to/keystore"));

        ConfigEntry plainJaasConfig = new ConfigEntry("listener.name.internal.plain." + SASL_JAAS_CONFIG, SENSITIVE_VALUE);
        verifyAlterConfig(brokerResource, List.of(plainJaasConfig), Map.of(plainJaasConfig.name(), Password.HIDDEN));

        ConfigEntry plainLoginCallback = new ConfigEntry("listener.name.internal.plain." + SASL_LOGIN_CALLBACK_HANDLER_CLASS, "test.LoginClass");
        verifyAlterConfig(brokerResource, List.of(plainLoginCallback), Map.of(plainLoginCallback.name(), plainLoginCallback.value()));

        ConfigEntry customConfig = new ConfigEntry("custom.config", SENSITIVE_VALUE);
        verifyAlterConfig(brokerResource, List.of(customConfig), Map.of(customConfig.name(), Password.HIDDEN));

        ConfigResource topicResource = new ConfigResource(ConfigResource.Type.TOPIC, "testTopic");
        ConfigEntry compressionType = new ConfigEntry(TopicConfig.COMPRESSION_TYPE_CONFIG, "lz4");
        verifyAlterConfig(topicResource, List.of(compressionType), Map.of(TopicConfig.COMPRESSION_TYPE_CONFIG, "lz4"));
        verifyAlterConfig(topicResource, List.of(customConfig), Map.of(customConfig.name(), Password.HIDDEN));

        // Verify empty request
        Request alterConfigs = request(new AlterConfigsRequest.Builder(Map.of(), true).build());
        assertEquals(Map.of(), ((AlterConfigsRequest) alterConfigs.loggableRequest()).configs());
    }

    private void verifyAlterConfig(ConfigResource resource, List<ConfigEntry> entries, Map<String, String> expectedValues) {
        AlterConfigsRequest alterConfigs = new AlterConfigsRequest.Builder(
            Map.of(resource, new Config(entries)), 
            true
        ).build();

        String alterConfigsString = alterConfigs.toString();
        entries.forEach(entry -> {
            if (!alterConfigsString.contains(entry.name())) {
                fail("Config names should be in the request string");
            }
            if (entry.value() != null && alterConfigsString.contains(entry.value())) {
                fail("Config values should not be in the request string");
            }
        });
        Request alterConfigsReq = request(alterConfigs);
        AlterConfigsRequest loggableAlterConfigs = (AlterConfigsRequest) alterConfigsReq.loggableRequest();
        Config loggedConfig = loggableAlterConfigs.configs().get(resource);
        assertEquals(expectedValues, toMap(loggedConfig));
        String alterConfigsDesc = RequestConvertToJson.requestDesc(
            alterConfigsReq.header(), 
            alterConfigsReq.requestLog(), 
            alterConfigsReq.isForwarded()
        ).toString();
        assertFalse(alterConfigsDesc.contains(SENSITIVE_VALUE), "Sensitive config logged " + alterConfigsDesc);
    }

    @Test
    public void testIncrementalAlterRequests() {
        ConfigResource brokerResource = new ConfigResource(ConfigResource.Type.BROKER, "1");
        Map<String, String> keystorePassword = Map.of(SSL_KEYSTORE_PASSWORD_CONFIG, SENSITIVE_VALUE);
        verifyIncrementalConfig(brokerResource, OpType.SET, keystorePassword, Map.of(SSL_KEYSTORE_PASSWORD_CONFIG, Password.HIDDEN));

        Map<String, String> keystoreLocation = Map.of(SSL_KEYSTORE_LOCATION_CONFIG, "/path/to/keystore");
        verifyIncrementalConfig(brokerResource, OpType.SET, keystoreLocation, keystoreLocation);
        verifyIncrementalConfig(
            brokerResource, 
            OpType.SET,
            Map.of(
                SSL_KEYSTORE_LOCATION_CONFIG, "/path/to/keystore",
                SSL_KEYSTORE_PASSWORD_CONFIG, SENSITIVE_VALUE
            ),
            Map.of(
                SSL_KEYSTORE_LOCATION_CONFIG, "/path/to/keystore",
                SSL_KEYSTORE_PASSWORD_CONFIG, Password.HIDDEN
            )
        );

        Map<String, String> listenerKeyPassword = Map.of("listener.name.internal." + SSL_KEY_PASSWORD_CONFIG, SENSITIVE_VALUE);
        verifyIncrementalConfig(brokerResource, OpType.SET, listenerKeyPassword, Map.of("listener.name.internal." + SSL_KEY_PASSWORD_CONFIG, Password.HIDDEN));

        Map<String, String> listenerKeystore = Map.of("listener.name.internal." + SSL_KEYSTORE_LOCATION_CONFIG, "/path/to/keystore");
        verifyIncrementalConfig(brokerResource, OpType.SET, listenerKeystore, listenerKeystore);

        Map<String, String> plainJaasConfig = Map.of("listener.name.internal.plain." + SASL_JAAS_CONFIG, SENSITIVE_VALUE);
        verifyIncrementalConfig(brokerResource, OpType.SET, plainJaasConfig, Map.of("listener.name.internal.plain." + SASL_JAAS_CONFIG, Password.HIDDEN));

        Map<String, String> plainLoginCallback = Map.of("listener.name.internal.plain." + SASL_LOGIN_CALLBACK_HANDLER_CLASS, "test.LoginClass");
        verifyIncrementalConfig(brokerResource, OpType.SET, plainLoginCallback, plainLoginCallback);

        Map<String, String> sslProtocols = Map.of(SSL_ENABLED_PROTOCOLS_CONFIG, "TLSv1.1");
        verifyIncrementalConfig(brokerResource, OpType.APPEND, sslProtocols, Map.of(SSL_ENABLED_PROTOCOLS_CONFIG, "TLSv1.1"));
        verifyIncrementalConfig(brokerResource, OpType.SUBTRACT, sslProtocols, Map.of(SSL_ENABLED_PROTOCOLS_CONFIG, "TLSv1.1"));
        Map<String, String> cipherSuites = Collections.singletonMap(SSL_CIPHER_SUITES_CONFIG, null);
        verifyIncrementalConfig(brokerResource, OpType.DELETE, cipherSuites, cipherSuites);

        Map<String, String> customConfig = Map.of("custom.config", SENSITIVE_VALUE);
        verifyIncrementalConfig(brokerResource, OpType.SET, customConfig, Map.of("custom.config", Password.HIDDEN));

        ConfigResource topicResource = new ConfigResource(ConfigResource.Type.TOPIC, "testTopic");
        Map<String, String> compressionType = Map.of(TopicConfig.COMPRESSION_TYPE_CONFIG, "lz4");
        verifyIncrementalConfig(topicResource, OpType.SET, compressionType, compressionType);
        verifyIncrementalConfig(topicResource, OpType.SET, customConfig, Map.of("custom.config", Password.HIDDEN));
    }

    private IncrementalAlterConfigsRequest incrementalAlterConfigs(
        ConfigResource resource,
        Map<String, String> entries,
        OpType op
    ) {
        IncrementalAlterConfigsRequestData data = new IncrementalAlterConfigsRequestData();
        AlterableConfigCollection alterableConfigs = new AlterableConfigCollection();
        entries.forEach((name, value) ->
            alterableConfigs.add(new AlterableConfig().setName(name).setValue(value).setConfigOperation(op.id())));
        data.resources().add(new AlterConfigsResource()
            .setResourceName(resource.name()).setResourceType(resource.type().id())
            .setConfigs(alterableConfigs));
        return new IncrementalAlterConfigsRequest.Builder(data).build();
    }

    private void verifyIncrementalConfig(
        ConfigResource resource,
        OpType op,
        Map<String, String> entries,
        Map<String, String> expectedValues
    ) {
        IncrementalAlterConfigsRequest alterConfigs = incrementalAlterConfigs(resource, entries, op);
        String alterConfigsString = alterConfigs.toString();
        entries.forEach((name, value) -> {
            if (!alterConfigsString.contains(name)) {
                fail("Config names should be in the request string");
            }
            if (value != null && alterConfigsString.contains(value)) {
                fail("Config values should not be in the request string");
            }
        });
        Request req = request(alterConfigs);
        IncrementalAlterConfigsRequest loggableAlterConfigs = (IncrementalAlterConfigsRequest) req.loggableRequest();
        AlterableConfigCollection loggedConfig = loggableAlterConfigs.data().resources()
            .find(resource.type().id(), resource.name()).configs();
        assertEquals(expectedValues, toMap(loggedConfig));
        String alterConfigsDesc = RequestConvertToJson.requestDesc(
            req.header(), req.requestLog(), req.isForwarded()).toString();
        assertFalse(alterConfigsDesc.contains(SENSITIVE_VALUE), "Sensitive config logged " + alterConfigsDesc);
    }

    @Test
    public void testNonAlterRequestsNotTransformed() {
        Request metadataRequest = request(new MetadataRequest.Builder(List.of("topic"), true).build());
        assertSame(metadataRequest.body(MetadataRequest.class), metadataRequest.loggableRequest());
    }

    @Test
    public void testJsonRequests() {
        ConfigResource resource = new ConfigResource(ConfigResource.Type.BROKER, "1");
        ConfigEntry keystorePassword = new ConfigEntry(SSL_KEYSTORE_PASSWORD_CONFIG, SENSITIVE_VALUE);
        List<ConfigEntry> entries = List.of(keystorePassword);

        Request alterConfigs = request(new AlterConfigsRequest.Builder(Map.of(resource, new Config(entries)), true).build());

        assertTrue(isValidJson(RequestConvertToJson.request(alterConfigs.loggableRequest()).toString()));
    }

    @ParameterizedTest
    @EnumSource(value = Errors.class, names = {"NONE", "CLUSTER_AUTHORIZATION_FAILED", "NOT_CONTROLLER"})
    public void testBuildEnvelopeResponse(Errors error) throws UnknownHostException {
        String topic = "foo";
        CreateTopicsRequest createTopicRequest = buildCreateTopicRequest(topic);
        Request unwrapped = buildUnwrappedEnvelopeRequest(createTopicRequest);

        CreateTopicsResponse createTopicResponse = buildCreateTopicResponse(topic, error);
        EnvelopeResponse envelopeResponse = buildEnvelopeResponse(unwrapped, createTopicResponse);

        if (error == Errors.NOT_CONTROLLER) {
            assertEquals(Errors.NOT_CONTROLLER, envelopeResponse.error());
            assertNull(envelopeResponse.responseData());
        } else {
            assertEquals(Errors.NONE, envelopeResponse.error());
            CreateTopicsResponse unwrappedResponse = (CreateTopicsResponse)
                AbstractResponse.parseResponse(envelopeResponse.responseData(), unwrapped.header());
            assertEquals(createTopicResponse.data(), unwrappedResponse.data());
        }
    }

    private CreateTopicsRequest buildCreateTopicRequest(String topic) {
        CreateTopicsRequestData requestData = new CreateTopicsRequestData();
        requestData.topics().add(new CreatableTopic()
            .setName(topic)
            .setReplicationFactor((short) -1)
            .setNumPartitions(-1)
        );
        return new CreateTopicsRequest.Builder(requestData).build();
    }

    private CreateTopicsResponse buildCreateTopicResponse(String topic, Errors error) {
        CreateTopicsResponseData responseData = new CreateTopicsResponseData();
        responseData.topics().add(new CreateTopicsResponseData.CreatableTopicResult()
            .setName(topic)
            .setErrorCode(error.code())
        );
        return new CreateTopicsResponse(responseData);
    }

    private Request buildUnwrappedEnvelopeRequest(AbstractRequest request) throws UnknownHostException {
        Request wrappedRequest = buildEnvelopeRequest(
            request,
            principalSerde,
            requestChannelMetrics,
            System.nanoTime()
        );
        AtomicReference<Request> unwrappedRequest = new AtomicReference<>();
        EnvelopeUtils.handleEnvelopeRequest(
            wrappedRequest,
            requestChannelMetrics,
            unwrappedRequest::set
        );

        return unwrappedRequest.get();
    }

    private Request buildEnvelopeRequest(
        AbstractRequest request,
        KafkaPrincipalSerde principalSerde,
        RequestChannelMetrics requestChannelMetrics,
        long startTimeNanos
    ) throws UnknownHostException {
        String clientId = "id";
        ListenerName listenerName = ListenerName.forSecurityProtocol(SecurityProtocol.PLAINTEXT);

        RequestHeader requestHeader = new RequestHeader(request.apiKey(), request.version(), clientId, 0);
        ByteBuffer requestBuffer = request.serializeWithHeader(requestHeader);

        RequestHeader envelopeHeader = new RequestHeader(ApiKeys.ENVELOPE, ApiKeys.ENVELOPE.latestVersion(), clientId, 0);
        ByteBuffer envelopeBuffer = new EnvelopeRequest.Builder(
            requestBuffer,
            principalSerde.serialize(KafkaPrincipal.ANONYMOUS),
            InetAddress.getLocalHost().getAddress()
        ).build().serializeWithHeader(envelopeHeader);

        RequestHeader.parse(envelopeBuffer);

        RequestContext envelopeContext = new RequestContext(envelopeHeader, "1", InetAddress.getLocalHost(), Optional.empty(),
            KafkaPrincipal.ANONYMOUS, listenerName, SecurityProtocol.PLAINTEXT, ClientInformation.EMPTY,
                true, Optional.of(principalSerde));

        Request envelopeRequest = new Request(
            1,
            envelopeContext,
            startTimeNanos,
            MemoryPool.NONE,
            envelopeBuffer,
            requestChannelMetrics,
            Optional.empty()
        );
        envelopeRequest.requestDequeueTimeNanos(-1);
        return envelopeRequest;
    }

    private boolean isValidJson(String str) {
        try {
            ObjectMapper mapper = new ObjectMapper();
            mapper.readTree(str);
            return true;
        } catch (IOException e) {
            return false;
        }
    }

    private Request request(AbstractRequest req) {
        ByteBuffer buffer = req.serializeWithHeader(new RequestHeader(req.apiKey(), req.version(), "client-id", 1));
        RequestContext requestContext = new RequestContext(
            RequestHeader.parse(buffer),
            "connection-id",
            InetAddress.getLoopbackAddress(),
            new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "user"),
            ListenerName.forSecurityProtocol(SecurityProtocol.PLAINTEXT),
            SecurityProtocol.PLAINTEXT,
            new ClientInformation("name", "version"),
            false
        );
        return new Request(1, requestContext, 0, mock(MemoryPool.class), buffer, mock(RequestChannelMetrics.class));
    }

    private Map<String, String> toMap(Config config) {
        Map<String, String> result = new LinkedHashMap<>();
        config.entries().forEach(e -> result.put(e.name(), e.value()));
        return result;
    }

    private Map<String, String> toMap(AlterableConfigCollection config) {
        Map<String, String> result = new LinkedHashMap<>();
        config.forEach(e -> result.put(e.name(), e.value()));
        return result;
    }

    private EnvelopeResponse buildEnvelopeResponse(Request unwrapped, AbstractResponse response) {
        assertTrue(unwrapped.envelope().isPresent());
        Request envelope = unwrapped.envelope().get();

        Send send = unwrapped.buildResponseSend(response);
        ByteBuffer sendBytes = ByteBufferChannel.toBuffer(send);

        // We need to read the size field before `parseResponse` below
        int size = sendBytes.getInt();
        assertEquals(size, sendBytes.remaining());
        AbstractResponse envelopeResponse = AbstractResponse.parseResponse(sendBytes, envelope.header());

        assertInstanceOf(EnvelopeResponse.class, envelopeResponse);
        return (EnvelopeResponse) envelopeResponse;
    }
}
