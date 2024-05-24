package org.apache.kafka.streams.processor.internals.testutil;

import org.apache.kafka.streams.errors.ProcessingExceptionHandler;
import org.apache.kafka.streams.processor.ErrorHandlerContext;
import org.apache.kafka.streams.processor.api.Record;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Map;

import static org.junit.Assert.*;

public class LogAndContinueOnInvalidDsl implements ProcessingExceptionHandler {

    @Override
    public ProcessingHandlerResponse handle(final ErrorHandlerContext context, final Record<?, ?> record, final Exception exception) {

        assertArrayEquals(new byte[] {0, 0, 0, 1}, context.sourceRawKey());
        assertArrayEquals(new byte[] {0, 0, 0, 1}, context.sourceRawValue());
        assertEquals(1, record.key());
        assertEquals("mv1", record.value());
        assertEquals("TOPIC_NAME", context.topic());
        assertEquals("KSTREAM-MAP-0000000002", context.processorNodeId());

        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        exception.printStackTrace(pw);
        assertTrue(sw.toString().contains("java.lang.StringIndexOutOfBoundsException: begin 0, end 4, length 3"));

        return ProcessingHandlerResponse.CONTINUE;
    }

    @Override
    public void configure(final Map<String, ?> configs) {
        // No-op
    }
}
