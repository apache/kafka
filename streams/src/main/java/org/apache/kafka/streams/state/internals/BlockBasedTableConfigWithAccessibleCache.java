package org.apache.kafka.streams.state.internals;

import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.Cache;

public class BlockBasedTableConfigWithAccessibleCache extends BlockBasedTableConfig {

    private Cache blockCache = null;

    @Override
    public BlockBasedTableConfig setBlockCache(final Cache cache) {
        blockCache = cache;
        return super.setBlockCache(cache);
    }

    public Cache blockCache() {
        return blockCache;
    }
}
