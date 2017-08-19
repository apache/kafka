package org.apache.kafka.streams.kstream.internals.onetomany;

import org.apache.kafka.streams.kstream.ValueJoiner;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.apache.kafka.streams.kstream.internals.Change;
import org.apache.kafka.streams.kstream.internals.KTableProcessorSupplier;
import org.apache.kafka.streams.kstream.internals.KTableValueGetter;
import org.apache.kafka.streams.kstream.internals.KTableValueGetterSupplier;
import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueIterator;


    
public class KTableJoinMergeProcessorSupplier<K,V,KL,VL,KR,VR> implements KTableProcessorSupplier<K, V, V>{
	private boolean sendOldValue = false;


	private KTableValueGetterSupplier<KL, VL> leftAccessor;
	private KTableValueGetterSupplier<K, VR> rightAccessor;
	private ValueMapper<K, KL>leftKeyExtractor;
	private ValueJoiner<VL, VR, V> joiner;

    public KTableJoinMergeProcessorSupplier(KTableValueGetterSupplier<KL, VL> byRange,
    										KTableValueGetterSupplier<K, VR> joinThis, 
											ValueMapper<K, KL>leftKeyExtractor, 
											ValueJoiner<VL, VR, V> joiner) {
		this.leftAccessor = byRange;
		this.rightAccessor = joinThis;
		this.leftKeyExtractor = leftKeyExtractor;
		this.joiner = joiner;
	}


	@Override
    public Processor<K, Change<V>> get() {
        return new AbstractProcessor<K, Change<V>>() {

            @Override
            public void process(K key, Change<V> value) {
                if (sendOldValue) {
                    context().forward(key, value);
                } else {
                    context().forward(key, new Change<>(value.newValue, null));
                }
            }
        };
    }

    @Override
    public KTableValueGetterSupplier<K, V> view() {
        final KTableValueGetter<KL, VL> leftGetter =  leftAccessor.get();
        final KTableValueGetter<K, VR> rightRepartitionedGetter = rightAccessor.get();
        
        return new KTableValueGetterSupplier<K, V>() {

            @Override
            public KTableValueGetter<K, V> get() {
                // TODO Auto-generated method stub
                return new KTableValueGetter<K, V>() {

                    @Override
                    public void init(ProcessorContext context) {
                    	/* initialize here?
                    	leftGetter.init(context);
                    	rightRepartitionedGetter.init(context);
                    	*/
                    }

                    @Override
                    public V get(K key) {
                        VL leftvalue = leftGetter.get(leftKeyExtractor.apply(key));
                        VR rigthValue = rightRepartitionedGetter.get(key);
                        if (leftvalue != null && rigthValue != null) { //INNER JOIN
                            return joiner.apply(leftvalue, rigthValue);
                        } else {
                            return null;
                        }

                    }
                };
            }

			@Override
			public String[] storeNames() {
				String[] leftNames = leftAccessor.storeNames();
				String[] right = rightAccessor.storeNames();
				String[] result = new String[leftNames.length + right.length];
				return null;
			}
        };
    }

	@Override
	public void enableSendingOldValues() {
		this.sendOldValue = true;
	}
}
	