package kafka.common.record;

import static kafka.common.utils.Utils.toArray;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.junit.Test;

public class MemoryRecordsTest {

    @Test
    public void testIterator() {
        MemoryRecords recs1 = new MemoryRecords(ByteBuffer.allocate(1024));
        MemoryRecords recs2 = new MemoryRecords(ByteBuffer.allocate(1024));
        List<Record> list = Arrays.asList(new Record("a".getBytes(), "1".getBytes()),
                                          new Record("b".getBytes(), "2".getBytes()),
                                          new Record("c".getBytes(), "3".getBytes()));
        for (int i = 0; i < list.size(); i++) {
            Record r = list.get(i);
            recs1.append(i, r);
            recs2.append(i, toArray(r.key()), toArray(r.value()), r.compressionType());
        }

        for (int iteration = 0; iteration < 2; iteration++) {
            for (MemoryRecords recs : Arrays.asList(recs1, recs2)) {
                Iterator<LogEntry> iter = recs.iterator();
                for (int i = 0; i < list.size(); i++) {
                    assertTrue(iter.hasNext());
                    LogEntry entry = iter.next();
                    assertEquals((long) i, entry.offset());
                    assertEquals(list.get(i), entry.record());
                }
                assertFalse(iter.hasNext());
            }
        }
    }

}
