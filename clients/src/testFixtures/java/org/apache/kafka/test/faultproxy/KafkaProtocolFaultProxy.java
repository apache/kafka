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
package org.apache.kafka.test.faultproxy;

import org.apache.kafka.common.message.FetchResponseData;
import org.apache.kafka.common.message.ProduceResponseData;
import org.apache.kafka.common.message.ResponseHeaderData;
import org.apache.kafka.common.message.TxnOffsetCommitResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.AddOffsetsToTxnResponse;
import org.apache.kafka.common.requests.EndTxnResponse;
import org.apache.kafka.common.requests.FetchResponse;
import org.apache.kafka.common.requests.FindCoordinatorResponse;
import org.apache.kafka.common.requests.InitProducerIdResponse;
import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.requests.RequestUtils;
import org.apache.kafka.common.requests.TxnOffsetCommitResponse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.EnumMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;

/**
 * A lightweight, client-agnostic Kafka wire-protocol fault-injection proxy for fast integration tests.
 *
 * <p>Sit it in front of a real (embedded) broker and point any client's {@code bootstrap.servers} at it;
 * it decodes requests/responses with Kafka's own protocol classes ({@link RequestHeader},
 * {@link AbstractResponse}, {@link RequestUtils#serialize}) — so it is correct across every wire version,
 * including flexible/tagged-field ones, with no hand-rolled byte offsets.
 *
 * <h2>Usage</h2>
 * <pre>{@code
 * try (var broker = new EmbeddedKafkaCluster(1)) {
 *     broker.start();
 *     try (var proxy = KafkaProtocolFaultProxy.inFrontOf(broker.bootstrapServers())) {
 *         // point clients here:
 *         props.put(BOOTSTRAP_SERVERS_CONFIG, proxy.bootstrapServers());
 *
 *         proxy.injectError(ApiKeys.END_TXN, Errors.CONCURRENT_TRANSACTIONS).once();
 *         proxy.injectError(ApiKeys.PRODUCE, Errors.NOT_ENOUGH_REPLICAS).onCall(2);
 *         proxy.disconnectOn(ApiKeys.END_TXN).once();      // the EOS "commit gap"
 *         proxy.delayOn(ApiKeys.FETCH, Duration.ofSeconds(2)).everyTime();  // slow broker
 *     }
 * }
 * }</pre>
 *
 * <p>Routing is transparent: the proxy rewrites {@code Metadata}/{@code FindCoordinator} responses so every
 * advertised address points back at itself, so a single-broker embedded cluster needs no special config
 * (its own ephemeral port is discovered from {@code bootstrapServers()}).
 *
 * <p>Determinism: {@code once()}/{@code onCall(n)}/{@code times(n)} are deterministic and safe for
 * assertions; {@code withProbability(p)} is chaos-mode only. The proxy never closes sockets unless a
 * {@code disconnectOn(...)} rule fires, so it is not itself a source of flakiness.
 */
public final class KafkaProtocolFaultProxy implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaProtocolFaultProxy.class);

    /**
     * Per-API setter that stamps an injected {@link Errors} onto a decoded response. Only APIs registered
     * here support {@code injectError(...)}; anything else fails fast at rule registration.
     */
    private static final Map<ApiKeys, BiConsumer<AbstractResponse, Errors>> ERROR_SETTERS = new EnumMap<>(ApiKeys.class);
    static {
        ERROR_SETTERS.put(ApiKeys.END_TXN, (r, e) -> ((EndTxnResponse) r).data().setErrorCode(e.code()));
        ERROR_SETTERS.put(ApiKeys.INIT_PRODUCER_ID, (r, e) -> ((InitProducerIdResponse) r).data().setErrorCode(e.code()));
        ERROR_SETTERS.put(ApiKeys.ADD_OFFSETS_TO_TXN, (r, e) -> ((AddOffsetsToTxnResponse) r).data().setErrorCode(e.code()));
        // TxnOffsetCommit carries the consumed offsets into the transaction. Under EOS-v2 / transactions V2
        // (KIP-890) the client sends this directly (AddOffsetsToTxn is skipped -- see TransactionManager
        // #sendOffsetsToTransaction), so this is THE offset-commit-into-txn RPC to fault for KIP-1035. The
        // response carries per-partition error codes, so stamp every partition of every topic.
        ERROR_SETTERS.put(ApiKeys.TXN_OFFSET_COMMIT, (r, e) -> {
            final TxnOffsetCommitResponseData data = ((TxnOffsetCommitResponse) r).data();
            data.topics().forEach(topic ->
                topic.partitions().forEach(p -> p.setErrorCode(e.code())));
        });
        ERROR_SETTERS.put(ApiKeys.PRODUCE, (r, e) -> {
            final ProduceResponseData data = ((org.apache.kafka.common.requests.ProduceResponse) r).data();
            data.responses().forEach(topic ->
                topic.partitionResponses().forEach(p -> p.setErrorCode(e.code())));
        });
        // FETCH stamps the error on every partition of the response. Because a fetch fault is almost always
        // scoped with forClient("restore") (or another clientId), this hits only the targeted consumer's
        // fetches — e.g. inject OFFSET_OUT_OF_RANGE on the restore consumer to exercise the restore path.
        ERROR_SETTERS.put(ApiKeys.FETCH, (r, e) -> {
            final FetchResponseData data = ((FetchResponse) r).data();
            data.responses().forEach(topic ->
                topic.partitions().forEach(p -> p.setErrorCode(e.code())));
        });
    }

    private final String targetHost;
    private final int targetPort;
    private final ExecutorService threadPool = Executors.newCachedThreadPool(r -> {
        final Thread t = new Thread(r, "kafka-fault-proxy");
        t.setDaemon(true);
        return t;
    });
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final CopyOnWriteArrayList<FaultRule> rules = new CopyOnWriteArrayList<>();
    private final Set<String> blackholedClients = ConcurrentHashMap.newKeySet();
    private ServerSocket serverSocket;
    private volatile String proxyHost;
    private volatile int proxyPort;

    private KafkaProtocolFaultProxy(final String targetBootstrap) {
        final String hostPort = targetBootstrap.split(",")[0].trim();
        final int idx = hostPort.lastIndexOf(':');
        this.targetHost = hostPort.substring(0, idx);
        this.targetPort = Integer.parseInt(hostPort.substring(idx + 1));
    }

    /** Create and start a proxy in front of the given broker bootstrap address. */
    public static KafkaProtocolFaultProxy inFrontOf(final String targetBootstrap) {
        final KafkaProtocolFaultProxy proxy = new KafkaProtocolFaultProxy(targetBootstrap);
        try {
            proxy.start();
        } catch (final Exception e) {
            throw new RuntimeException("Failed to start fault proxy", e);
        }
        return proxy;
    }

    private void start() throws Exception {
        serverSocket = new ServerSocket(0);
        proxyPort = serverSocket.getLocalPort();
        proxyHost = "localhost";
        running.set(true);
        threadPool.submit(this::acceptLoop);
        LOG.info("Fault proxy listening on {}:{} -> broker {}:{}", proxyHost, proxyPort, targetHost, targetPort);
    }

    /** The address to hand to a client's {@code bootstrap.servers}. */
    public String bootstrapServers() {
        return proxyHost + ":" + proxyPort;
    }

    // ------------------------------------------------------------------
    // DSL
    // ------------------------------------------------------------------

    /** Rewrite responses of {@code apiKey} to carry {@code error}. Follow with a trigger, e.g. {@code .once()}. */
    public FaultRule.Builder injectError(final ApiKeys apiKey, final Errors error) {
        if (!ERROR_SETTERS.containsKey(apiKey)) {
            throw new IllegalArgumentException("Error injection is not supported for " + apiKey
                    + " yet. Supported: " + ERROR_SETTERS.keySet()
                    + " (add a setter to ERROR_SETTERS to extend).");
        }
        if (apiKey == ApiKeys.METADATA || apiKey == ApiKeys.FIND_COORDINATOR) {
            throw new IllegalArgumentException(apiKey + " is reserved for routing and cannot carry injected errors.");
        }
        return new FaultRule.Builder(this, apiKey, FaultRule.Action.INJECT_ERROR, error, 0L);
    }

    /** Drop the connection when a response of {@code apiKey} would be returned (models the EOS commit gap). */
    public FaultRule.Builder disconnectOn(final ApiKeys apiKey) {
        return new FaultRule.Builder(this, apiKey, FaultRule.Action.DISCONNECT, null, 0L);
    }

    /**
     * Hold back a response of {@code apiKey} by {@code delay} before forwarding it to the client, modelling a
     * slow broker. Only the matching response on its own connection is delayed (each connection/direction runs
     * on its own thread), so other clients and the request path are unaffected. Follow with a trigger, e.g.
     * {@code .everyTime()}. A delay longer than the client's {@code request.timeout.ms} will be seen by the
     * client as a timeout (and handled like a disconnect), so size the delay against the timeout under test.
     */
    public FaultRule.Builder delayOn(final ApiKeys apiKey, final Duration delay) {
        final long millis = delay.toMillis();
        if (millis < 0) {
            throw new IllegalArgumentException("delay must not be negative: " + delay);
        }
        return new FaultRule.Builder(this, apiKey, FaultRule.Action.DELAY, null, millis);
    }

    /**
     * Blackhole every request from clients whose clientId contains {@code clientIdSubstring}: drop the request
     * before it reaches the broker and close the connection. The broker stops hearing that instance's
     * heartbeats and evicts it by session timeout — an ungraceful, reversible one-node network partition.
     * Set a distinct {@code client.id} per Streams instance to target one instance. Undo via {@link #clearFaults()}.
     */
    public void blackholeClient(final String clientIdSubstring) {
        blackholedClients.add(clientIdSubstring);
    }

    /** Remove all registered faults and client blackholes (routing rewrites are always on and unaffected). */
    public void clearFaults() {
        rules.clear();
        blackholedClients.clear();
    }

    void addFault(final FaultRule rule) {
        rules.add(rule);
    }

    void removeFault(final FaultRule rule) {
        rules.remove(rule);
    }

    // ------------------------------------------------------------------
    // Proxy internals
    // ------------------------------------------------------------------

    private void acceptLoop() {
        while (running.get()) {
            try {
                final Socket client = serverSocket.accept();
                final Socket broker = new Socket(targetHost, targetPort);
                final Connection conn = new Connection();
                threadPool.submit(() -> pumpRequests(client, broker, conn));
                threadPool.submit(() -> pumpResponses(broker, client, conn));
            } catch (final Exception e) {
                if (running.get()) {
                    LOG.warn("accept loop error", e);
                }
            }
        }
    }

    /** Per-connection state: correlationId -> request header, so responses can be decoded/matched. */
    private static final class Connection {
        private final Map<Integer, RequestHeader> inflight = new ConcurrentHashMap<>();
    }

    // client -> broker: forward verbatim, recording each request header for response decoding. If the
    // connection's clientId is blackholed, drop the request (do NOT forward) and close the connection — this
    // simulates a one-node network partition on the REQUEST path, so the broker stops hearing that instance's
    // heartbeats and evicts it by session timeout (the ungraceful-crash path). Reversible via clearFaults().
    private void pumpRequests(final Socket client, final Socket broker, final Connection conn) {
        try (client; broker;
             DataInputStream in = new DataInputStream(client.getInputStream());
             DataOutputStream out = new DataOutputStream(broker.getOutputStream())) {
            byte[] frame;
            while (running.get() && (frame = readFrame(in)) != null) {
                try {
                    final RequestHeader header = RequestHeader.parse(ByteBuffer.wrap(frame));
                    conn.inflight.put(header.correlationId(), header);
                    if (isBlackholed(header.clientId())) {
                        LOG.info("Fault: blackholing request {} from client {} (dropping, not forwarding)",
                                header.apiKey(), header.clientId());
                        break; // closes both sockets via try-with-resources; broker never sees this request
                    }
                } catch (final Exception parseErr) {
                    LOG.debug("could not parse request header (forwarding anyway)", parseErr);
                }
                writeFrame(out, frame);
            }
        } catch (final Exception e) {
            LOG.debug("request pump closed", e);
        }
    }

    private boolean isBlackholed(final String clientId) {
        if (clientId == null || blackholedClients.isEmpty()) {
            return false;
        }
        for (final String substring : blackholedClients) {
            if (clientId.contains(substring)) {
                return true;
            }
        }
        return false;
    }

    // broker -> client: rewrite for routing and/or apply a matching fault rule; otherwise forward verbatim.
    private void pumpResponses(final Socket broker, final Socket client, final Connection conn) {
        try (broker; client;
             DataInputStream in = new DataInputStream(broker.getInputStream());
             DataOutputStream out = new DataOutputStream(client.getOutputStream())) {
            byte[] frame;
            while (running.get() && (frame = readFrame(in)) != null) {
                final int correlationId = ByteBuffer.wrap(frame).getInt(0);
                final RequestHeader reqHeader = conn.inflight.remove(correlationId);

                if (reqHeader == null) {
                    writeFrame(out, frame);
                    continue;
                }

                final ApiKeys apiKey = reqHeader.apiKey();
                final boolean routing = apiKey == ApiKeys.METADATA || apiKey == ApiKeys.FIND_COORDINATOR;
                final FaultRule fired = firstFiringRule(apiKey, reqHeader.clientId());

                if (fired != null && fired.action() == FaultRule.Action.DISCONNECT) {
                    LOG.info("Fault: dropping connection on {} response ({})", apiKey, fired);
                    break; // closes both sockets via try-with-resources
                }

                if (fired != null && fired.action() == FaultRule.Action.DELAY) {
                    LOG.info("Fault: delaying {} response by {}ms ({})", apiKey, fired.delayMillis(), fired);
                    Thread.sleep(fired.delayMillis()); // blocks only this connection's response thread
                }

                if (!routing && (fired == null || fired.action() == FaultRule.Action.DELAY)) {
                    writeFrame(out, frame); // DELAY does not alter the bytes, only their timing
                    continue;
                }

                writeFrame(out, transform(reqHeader, frame, routing, fired));
            }
        } catch (final Exception e) {
            LOG.debug("response pump closed", e);
        }
    }

    // Decode -> (routing rewrite and/or error inject) -> re-encode. Falls back to the original bytes on error.
    private byte[] transform(final RequestHeader reqHeader, final byte[] frame,
                             final boolean routing, final FaultRule fired) {
        final ApiKeys apiKey = reqHeader.apiKey();
        final short version = reqHeader.apiVersion();
        try {
            final AbstractResponse response = AbstractResponse.parseResponse(ByteBuffer.wrap(frame), reqHeader);

            if (routing) {
                applyRouting(response);
            }
            if (fired != null && fired.action() == FaultRule.Action.INJECT_ERROR) {
                ERROR_SETTERS.get(apiKey).accept(response, fired.error());
                LOG.info("Fault: injected {} into {} response ({})", fired.error(), apiKey, fired);
            }

            final ResponseHeaderData headerData = new ResponseHeaderData().setCorrelationId(reqHeader.correlationId());
            final ByteBuffer bb = RequestUtils.serialize(
                    headerData, apiKey.responseHeaderVersion(version), response.data(), version);
            final byte[] out = new byte[bb.remaining()];
            bb.get(out);
            return out;
        } catch (final Exception e) {
            LOG.warn("failed to transform {} v{} response; forwarding verbatim", apiKey, version, e);
            return frame;
        }
    }

    private void applyRouting(final AbstractResponse response) {
        if (response instanceof MetadataResponse) {
            ((MetadataResponse) response).data().brokers().forEach(b -> b.setHost(proxyHost).setPort(proxyPort));
        } else if (response instanceof FindCoordinatorResponse) {
            final FindCoordinatorResponse fc = (FindCoordinatorResponse) response;
            fc.data().setHost(proxyHost).setPort(proxyPort);
            fc.data().coordinators().forEach(c -> c.setHost(proxyHost).setPort(proxyPort));
        }
    }

    private FaultRule firstFiringRule(final ApiKeys apiKey, final String clientId) {
        FaultRule chosen = null;
        for (final FaultRule rule : rules) {
            // Gate on apiKey AND clientId before shouldFire(), so a client-scoped rule only counts (and
            // fires on) matching requests — a fetch fault scoped to "restore" ignores the main consumer.
            if (rule.apiKey() == apiKey && rule.matchesClient(clientId) && rule.shouldFire() && chosen == null) {
                chosen = rule; // keep evaluating so every matching rule still counts its match
            }
        }
        return chosen;
    }

    /** Reads one length-prefixed Kafka frame (without the 4-byte length). Returns null on clean EOF/close. */
    private static byte[] readFrame(final DataInputStream in) {
        try {
            final int size = in.readInt();
            final byte[] frame = new byte[size];
            in.readFully(frame);
            return frame;
        } catch (final EOFException eof) {
            return null;
        } catch (final Exception e) {
            return null;
        }
    }

    private static void writeFrame(final DataOutputStream out, final byte[] frame) throws Exception {
        out.writeInt(frame.length);
        out.write(frame);
        out.flush();
    }

    @Override
    public void close() {
        running.set(false);
        try {
            if (serverSocket != null) {
                serverSocket.close();
            }
        } catch (final Exception ignored) {
            // closing
        }
        threadPool.shutdownNow();
    }
}
