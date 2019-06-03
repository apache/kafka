package org.apache.kafka.streams.state;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.kstream.internals.foreignkeyjoin.CombinedKey;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.NavigableMap;
import java.util.TreeMap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

public class WooloolooTest {

    @Test
    public void woolooloo() {
        final HashMap<String, String> result = new HashMap<>();
        final TreeMap<String, String> treeMap = new TreeMap<>(result);

        treeMap.put("woo", "loo");
        treeMap.put("woo2", "loo2");
        treeMap.put("wo", "lo");
        treeMap.put("woolooloo", "looloo");

        NavigableMap<String, String> ff = treeMap.subMap("woo", true, "woo" + Character.MAX_VALUE, true);

        ff.forEach( (key, value) -> System.out.println("key = " + key +", value = " + value));
        assertEquals(1, 1);
    }


    @Test
    public void bytesWooLooLoo() {
        final HashMap<Bytes, String> result = new HashMap<>();
        final TreeMap<Bytes, String> treeMap = new TreeMap<>(result);

        final Bytes startKey = new Bytes(new byte[]{(byte)0xFF});

        treeMap.put(new Bytes(new byte[]{(byte)0xFF, (byte)0x01}), "loo");
        treeMap.put(new Bytes(new byte[]{(byte)0xFE, (byte)0xFF}), "loo2");
        treeMap.put(startKey, "lo");

        final Bytes endKey = new Bytes(ArrayUtils.addAll(startKey.get(), Byte.MAX_VALUE));

        NavigableMap<Bytes, String> ff = treeMap.subMap(startKey, true, endKey, true);

        ff.forEach( (key, value) -> System.out.println("key = " + key +", value = " + value));
        assertEquals(1, 1);
    }
}
