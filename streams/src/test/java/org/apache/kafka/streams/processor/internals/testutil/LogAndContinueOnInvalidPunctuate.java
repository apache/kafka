package org.apache.kafka.streams.processor.internals.testutil;

import org.apache.kafka.streams.errors.ProcessingExceptionHandler;
import org.apache.kafka.streams.processor.ErrorHandlerContext;
import org.apache.kafka.streams.processor.api.Record;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Map;

import static org.junit.Assert.*;

public class LogAndContinueOnInvalidPunctuate implements ProcessingExceptionHandler {

    @Override
    public ProcessingHandlerResponse handle(final ErrorHandlerContext context, final Record<?, ?> record, final Exception exception) {

        assertNull(context.sourceRawKey());
        assertNull(context.sourceRawValue());
        assertNull(record);
        assertNull(context.topic());
        assertEquals("KSTREAM-PROCESSOR-0000000001", context.processorNodeId());

        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        exception.printStackTrace(pw);
        assertTrue(sw.toString().contains("java.lang.ArithmeticException: / by zero"));

        return ProcessingHandlerResponse.CONTINUE;
    }

    @Override
    public void configure(final Map<String, ?> configs) {
        // No-op
    }
}
