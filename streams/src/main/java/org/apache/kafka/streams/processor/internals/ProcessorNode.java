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
package org.apache.kafka.streams.processor.internals;

import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.streams.errors.ErrorHandlerContext;
import org.apache.kafka.streams.errors.ProcessingExceptionHandler;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.errors.TaskCorruptedException;
import org.apache.kafka.streams.errors.TaskMigratedException;
import org.apache.kafka.streams.errors.internals.DefaultErrorHandlerContext;
import org.apache.kafka.streams.errors.internals.FailedProcessingException;
import org.apache.kafka.streams.processor.api.FixedKeyProcessor;
import org.apache.kafka.streams.processor.api.FixedKeyProcessorContext;
import org.apache.kafka.streams.processor.api.InternalFixedKeyRecordFactory;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.processor.internals.metrics.TaskMetrics;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static org.apache.kafka.streams.StreamsConfig.PROCESSING_EXCEPTION_HANDLER_CLASS_CONFIG;

public class ProcessorNode<KIn, VIn, KOut, VOut> {

    private static final Logger log = LoggerFactory.getLogger(ProcessorNode.class);
    private final List<ProcessorNode<KOut, VOut, ?, ?>> children;
    private final Map<String, ProcessorNode<KOut, VOut, ?, ?>> childByName;

    private final Processor<KIn, VIn, KOut, VOut> processor;
    private final FixedKeyProcessor<KIn, VIn, VOut> fixedKeyProcessor;
    private final String name;

    public final Set<String> stateStores;
    private ProcessingExceptionHandler processingExceptionHandler;

    private InternalProcessorContext<KOut, VOut> internalProcessorContext;
    private String threadId;

    private boolean closed = true;

    private Sensor droppedRecordsSensor;

    public ProcessorNode(final String name) {
        this(name, (Processor<KIn, VIn, KOut, VOut>) null, null);
    }

    public ProcessorNode(final String name,
                         final Processor<KIn, VIn, KOut, VOut> processor,
                         final Set<String> stateStores) {

        this.name = name;
        this.processor = processor;
        this.fixedKeyProcessor = null;
        this.children = new ArrayList<>();
        this.childByName = new HashMap<>();
        this.stateStores = stateStores;
    }

    public ProcessorNode(final String name,
                         final FixedKeyProcessor<KIn, VIn, VOut> processor,
                         final Set<String> stateStores) {

        this.name = name;
        this.processor = null;
        this.fixedKeyProcessor = processor;
        this.children = new ArrayList<>();
        this.childByName = new HashMap<>();
        this.stateStores = stateStores;
    }

    public final String name() {
        return name;
    }

    public List<ProcessorNode<KOut, VOut, ?, ?>> children() {
        return children;
    }

    ProcessorNode<KOut, VOut, ?, ?> child(final String childName) {
        return childByName.get(childName);
    }

    public void addChild(final ProcessorNode<KOut, VOut, ?, ?> child) {
        children.add(child);
        childByName.put(child.name, child);
    }

    public void init(final InternalProcessorContext<KOut, VOut> context) {
        if (!closed)
            throw new IllegalStateException("The processor is not closed");

        try {
            threadId = Thread.currentThread().getName();
            internalProcessorContext = context;
            droppedRecordsSensor = TaskMetrics.droppedRecordsSensor(threadId,
                internalProcessorContext.taskId().toString(),
                internalProcessorContext.metrics());

            if (processor != null) {
                processor.init(context);
            }
            if (fixedKeyProcessor != null) {
                @SuppressWarnings("unchecked") final FixedKeyProcessorContext<KIn, VOut> fixedKeyProcessorContext =
                    (FixedKeyProcessorContext<KIn, VOut>) context;
                fixedKeyProcessor.init(fixedKeyProcessorContext);
            }
        } catch (final Exception e) {
            throw new StreamsException(String.format("failed to initialize processor %s", name), e);
        }

        // revived tasks could re-initialize the topology,
        // in which case we should reset the flag
        closed = false;
    }

    public void init(final InternalProcessorContext<KOut, VOut> context, final ProcessingExceptionHandler processingExceptionHandler) {
        init(context);
        this.processingExceptionHandler = processingExceptionHandler;
    }

    public void close() {
        throwIfClosed();

        try {
            if (processor != null) {
                processor.close();
            }
            if (fixedKeyProcessor != null) {
                fixedKeyProcessor.close();
            }
            internalProcessorContext.metrics().removeAllNodeLevelSensors(
                threadId,
                internalProcessorContext.taskId().toString(),
                name
            );
        } catch (final Exception e) {
            throw new StreamsException(String.format("failed to close processor %s", name), e);
        }

        closed = true;
    }

    protected void throwIfClosed() {
        if (closed) {
            throw new IllegalStateException("The processor is already closed");
        }
    }


    public void process(final Record<KIn, VIn> record) {
        throwIfClosed();

        try {
            if (processor != null) {
                processor.process(record);
            } else if (fixedKeyProcessor != null) {
                fixedKeyProcessor.process(
                    InternalFixedKeyRecordFactory.create(record)
                );
            } else {
                throw new IllegalStateException(
                    "neither the processor nor the fixed key processor were set."
                );
            }
        } catch (final ClassCastException e) {
            final String keyClass = record.key() == null ? "unknown because key is null" : record.key().getClass().getName();
            final String valueClass = record.value() == null ? "unknown because value is null" : record.value().getClass().getName();
            throw new StreamsException(String.format("ClassCastException invoking processor: %s. Do the Processor's "
                    + "input types match the deserialized types? Check the Serde setup and change the default Serdes in "
                    + "StreamConfig or provide correct Serdes via method parameters. Make sure the Processor can accept "
                    + "the deserialized input of type key: %s, and value: %s.%n"
                    + "Note that although incorrect Serdes are a common cause of error, the cast exception might have "
                    + "another cause (in user code, for example). For example, if a processor wires in a store, but casts "
                    + "the generics incorrectly, a class cast exception could be raised during processing, but the "
                    + "cause would not be wrong Serdes.",
                    this.name(),
                    keyClass,
                    valueClass),
                e);
        } catch (final FailedProcessingException | TaskCorruptedException | TaskMigratedException e) {
            // Rethrow exceptions that should not be handled here
            throw e;
        } catch (final Exception processingException) {
            // while Java distinguishes checked vs unchecked exceptions, other languages
            // like Scala or Kotlin do not, and thus we need to catch `Exception`
            // (instead of `RuntimeException`) to work well with those languages
            final ErrorHandlerContext errorHandlerContext = new DefaultErrorHandlerContext(
                null, // only required to pass for DeserializationExceptionHandler
                internalProcessorContext.topic(),
                internalProcessorContext.partition(),
                internalProcessorContext.offset(),
                internalProcessorContext.headers(),
                internalProcessorContext.currentNode().name(),
                internalProcessorContext.taskId(),
                internalProcessorContext.timestamp());

            final ProcessingExceptionHandler.ProcessingHandlerResponse response;
            try {
                response = Objects.requireNonNull(
                    processingExceptionHandler.handle(errorHandlerContext, record, processingException),
                    "Invalid ProductionExceptionHandler response."
                );
            } catch (final Exception fatalUserException) {
                // while Java distinguishes checked vs unchecked exceptions, other languages
                // like Scala or Kotlin do not, and thus we need to catch `Exception`
                // (instead of `RuntimeException`) to work well with those languages
                log.error(
                    "Processing error callback failed after processing error for record: {}",
                    errorHandlerContext,
                    processingException
                );
                throw new FailedProcessingException(
                    "Fatal user code error in processing error callback",
                    internalProcessorContext.currentNode().name(),
                    fatalUserException
                );
            }

            if (response == ProcessingExceptionHandler.ProcessingHandlerResponse.FAIL) {
                log.error("Processing exception handler is set to fail upon" +
                     " a processing error. If you would rather have the streaming pipeline" +
                     " continue after a processing error, please set the " +
                     PROCESSING_EXCEPTION_HANDLER_CLASS_CONFIG + " appropriately.");
                throw new FailedProcessingException(internalProcessorContext.currentNode().name(), processingException);
            } else {
                droppedRecordsSensor.record();
            }
        }
    }

    public boolean isTerminalNode() {
        return children.isEmpty();
    }

    /**
     * @return a string representation of this node, useful for debugging.
     */
    @Override
    public String toString() {
        return toString("");
    }

    /**
     * @return a string representation of this node starting with the given indent, useful for debugging.
     */
    public String toString(final String indent) {
        final StringBuilder sb = new StringBuilder(indent + name + ":\n");
        if (stateStores != null && !stateStores.isEmpty()) {
            sb.append(indent).append("\tstates:\t\t[");
            for (final String store : stateStores) {
                sb.append(store);
                sb.append(", ");
            }
            sb.setLength(sb.length() - 2);  // remove the last comma
            sb.append("]\n");
        }
        return sb.toString();
    }
}
