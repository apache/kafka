package org.apache.kafka.clients.producer.internals;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.Random;

import static org.junit.Assert.assertEquals;

@RunWith(PowerMockRunner.class)
public class MmapBufferPoolTest {
    private final long maxBlockTimeMs = 2000;

    @Test
    public void testSimple() throws Exception {
        long totalMemory = 64 * 1024;
        int size = 1024;
        File storefile = new File(System.getProperty("java.io.tmpdir"), "kafka-producer-data-" + new Random().nextInt(Integer.MAX_VALUE) + ".dat");
        BufferPool pool = new MmapBufferPool(storefile, totalMemory, size);
        ByteBuffer buffer = pool.allocate(size, maxBlockTimeMs);
        assertEquals("Buffer size should equal requested size.", size, buffer.limit());
        assertEquals("Unallocated memory should have shrunk", totalMemory - size, pool.unallocatedMemory());
        assertEquals("Available memory should have shrunk", totalMemory - size, pool.availableMemory());
    }
}
