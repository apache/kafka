package org.apache.kafka.streams.kstream.internals.onetomany;

import org.apache.kafka.streams.kstream.ValueJoiner;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.apache.kafka.streams.kstream.internals.Change;
import org.apache.kafka.streams.kstream.internals.KTableRangeValueGetterSupplier;
import org.apache.kafka.streams.kstream.internals.KTableSourceValueGetterSupplier;
import org.apache.kafka.streams.kstream.internals.KTableValueGetter;
import org.apache.kafka.streams.kstream.internals.KTableValueGetterSupplier;
import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.ProcessorSupplier;
import org.apache.kafka.streams.state.KeyValueStore;

public class RangeKeyValueGetterProviderAndProcessorSupplier<K0, V0, K, V, VO> implements ProcessorSupplier<K0, VO>
{

    String topicName;
    //    KTableSource<K, VR> kts;
    ValueMapper<K0, K> mapper;
    KTableValueGetterSupplier<K, V> leftValueGetterSupplier;
    ValueJoiner<V, VO, V0> joiner;

    public RangeKeyValueGetterProviderAndProcessorSupplier(String topicName, KTableValueGetterSupplier<K, V> leftValueGetter , ValueMapper<K0, K> mapper, ValueJoiner<V, VO, V0> joiner)
    {
        this.topicName = topicName;
        this.mapper = mapper;
        this.joiner = joiner;
	    this.leftValueGetterSupplier = leftValueGetter;
    }


    @Override
    public Processor<K0, VO> get()
    {

        return new AbstractProcessor<K0, VO>()
        {

            KeyValueStore<K0, VO> store;
            KTableValueGetter<K, V> leftValues;

            @Override
            public void init(ProcessorContext context)
            {
                super.init(context);
                leftValues = leftValueGetterSupplier.get();
                leftValues.init(context);
                store = (KeyValueStore<K0, VO>) context.getStateStore(topicName);
            }

            @Override
            public void process(K0 key, VO value)
            {
                VO oldVal = store.get(key);
                store.put(key, value);

                V0 newValue = null;
                V0 oldValue = null;
                V value2 = null;

                if (value != null || oldVal != null)
                    value2 = leftValues.get(mapper.apply(key));

                if (value != null && value2 != null)
                    newValue = joiner.apply(value2, value);

                if (oldVal != null && value2 != null)
                    oldValue = joiner.apply(value2, oldVal);

                if(oldValue != null || newValue != null)
                	context().forward(key, new Change<>(newValue, oldValue));
            }
        };
    }

    
    public KTableRangeValueGetterSupplier<K0, VO> valueGetterSupplier() {
    	return new KTableSourceValueGetterSupplier<K0, VO>(topicName);
    }
    
    
//    @Override
//    public KTableRangeValueGetter<K, VR> view()
//    {
//        return new KTableRangeValueGetter<K, VR>()
//        {
//
//            KeyValueStore<K, VR> store = null;
//
//            @SuppressWarnings("unchecked")
//            public void init(ProcessorContext context)
//            {
//                store = (KeyValueStore<K, VR>) context.getStateStore(topicName);
//            }
//
//            @Override
//            public KeyValueIterator<K, VR> getRange(final K key)
//            {
//
//                KeyValueIterator<K, VR> iter = store.range(key, key);
//                try {
//                    //serdes only initialized after openDB not after init
//                    Field serdesField;
//                    Field innerField;
//                    innerField = store.getClass().getDeclaredField("inner");
//                    innerField.setAccessible(true);
//                    Object innerStore = innerField.get(store);
//                    serdesField = innerStore.getClass().getDeclaredField("serdes");
//                    serdesField.setAccessible(true);
//                    final StateSerdes<K, VR> serdes = (StateSerdes<K, VR>) serdesField.get(innerStore);
//                    Field innerIterField = iter.getClass().getDeclaredField("iter");
//                    innerIterField.setAccessible(true);
//                    Object innerIter = innerIterField.get(iter);
//                    Field rocksIterField = innerIter.getClass().getSuperclass().getDeclaredField("iter");
//                    rocksIterField.setAccessible(true);
//                    final RocksIterator rocksIter = (RocksIterator) rocksIterField.get(innerIter);
//                    rocksIter.seek(serdes.rawKey(key)); //already prefix faked
//                    return new KeyValueIterator<K, VR>()
//                    {
//                        private final Comparator<byte[]> comparator = new Comparator<byte[]>()
//                        {
//
//                            // param2 is key form the range call, param1 is current key of the rocksiter
//                            // we are cheating here, we return 0 aslong as we are a prefix
//                            // and 1 if we stop beeing one
//                            @Override
//                            public int compare(byte[] o1, byte[] o2)
//                            {
//                                for (int i = 0; i < o2.length; i++) {
//                                    if (i == o1.length) {
//                                        throw new ArrayIndexOutOfBoundsException("WTF just happend? " + key.toString() + key.getClass());
//                                    }
//
//                                    if (o1[i] != o2[i]) {
//                                        return 1;
//                                    }
//
//                                }
//                                return 0;
//                            }
//                        };
//
//                        private byte[] rawToKey = serdes.rawKey(key);//already prefix faked
//
//                        @Override
//                        public boolean hasNext()
//                        {
//                            boolean hasNext =  rocksIter.isValid() && comparator.compare(rocksIter.key(), this.rawToKey) <= 0;
//                            if(!hasNext){
//                            	rocksIter.close(); //usually customers of us will not close us
//                            }
//                            return hasNext;
//                        }
//
//                        @Override
//                        public KeyValue<K, VR> next()
//                        {
//                            if (!hasNext())
//                                throw new NoSuchElementException();
//
//                            KeyValue<K, VR> entry = new KeyValue<>(serdes.keyFrom(rocksIter.key()), serdes.valueFrom(rocksIter.value()));
//                            rocksIter.next();
//                            return entry;
//                        }
//
//                        @Override
//                        public void remove()
//                        {
//                            throw new UnsupportedOperationException("RocksDB iterator does not support remove");
//
//                        }
//
//                        @Override
//                        public void close()
//                        {
//                            if(rocksIter.isValid()) {
//                            	rocksIter.close();
//                            }
//                        }
//                    };
//                } catch (NoSuchFieldException | SecurityException | IllegalArgumentException | IllegalAccessException e) {
//                    // TODO Auto-generated catch block
//                    e.printStackTrace();
//                }
//
//                return iter;
//
//            }
//
//            @Override
//            public VR get(K key)
//            {
//                return this.store.get(key);
//            }
//
//        };
//    }
}
