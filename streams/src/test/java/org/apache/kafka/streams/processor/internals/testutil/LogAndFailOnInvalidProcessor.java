package org.apache.kafka.streams.processor.internals.testutil;

import org.apache.kafka.streams.errors.ProcessingExceptionHandler;
import org.apache.kafka.streams.processor.ErrorHandlerContext;
import org.apache.kafka.streams.processor.api.Record;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Map;

import static org.junit.Assert.*;

public class LogAndFailOnInvalidProcessor implements ProcessingExceptionHandler {

    @Override
    public ProcessingHandlerResponse handle(final ErrorHandlerContext context, final Record<?, ?> record, final Exception exception) {

        assertArrayEquals(new byte[] {0, 0, 0, 0}, context.sourceRawKey());
        assertArrayEquals(new byte[] {0, 0, 0, 0}, context.sourceRawValue());
        assertEquals(0, record.key());
        assertEquals(0, record.value());
        assertEquals("TOPIC_NAME", context.topic());
        assertEquals("KSTREAM-PROCESSOR-0000000001", context.processorNodeId());

        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        exception.printStackTrace(pw);
        assertTrue(sw.toString().contains("java.lang.ArithmeticException: / by zero"));


        return ProcessingHandlerResponse.FAIL;
    }

    @Override
    public void configure(final Map<String, ?> configs) {
        // No-op
    }
}
