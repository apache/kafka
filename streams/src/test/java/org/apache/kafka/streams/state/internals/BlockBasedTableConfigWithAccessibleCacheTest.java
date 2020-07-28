package org.apache.kafka.streams.state.internals;

import org.junit.Test;
import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.Cache;
import org.rocksdb.LRUCache;
import org.rocksdb.RocksDB;

import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.CoreMatchers.sameInstance;
import static org.hamcrest.MatcherAssert.assertThat;

public class BlockBasedTableConfigWithAccessibleCacheTest {

    static {
        RocksDB.loadLibrary();
    }

    @Test
    public void shouldReturnNoBlockCacheIfNoneIsSet() {
        final BlockBasedTableConfigWithAccessibleCache configWithAccessibleCache =
            new BlockBasedTableConfigWithAccessibleCache();

        assertThat(configWithAccessibleCache.blockCache(), nullValue());
    }

    @Test
    public void shouldSetBlockCacheAndMakeItAccessible() {
        final BlockBasedTableConfigWithAccessibleCache configWithAccessibleCache =
            new BlockBasedTableConfigWithAccessibleCache();
        final Cache blockCache = new LRUCache(1024);

        final BlockBasedTableConfig updatedConfig = configWithAccessibleCache.setBlockCache(blockCache);

        assertThat(updatedConfig, sameInstance(configWithAccessibleCache));
        assertThat(configWithAccessibleCache.blockCache(), sameInstance(blockCache));
    }
}