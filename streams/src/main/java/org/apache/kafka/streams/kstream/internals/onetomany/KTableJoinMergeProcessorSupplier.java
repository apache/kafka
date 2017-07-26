package org.apache.kafka.streams.kstream.internals.onetomany;

import org.apache.kafka.streams.kstream.ValueJoiner;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.apache.kafka.streams.kstream.internals.Change;
import org.apache.kafka.streams.kstream.internals.KTableImpl;
import org.apache.kafka.streams.kstream.internals.KTableProcessorSupplier;
import org.apache.kafka.streams.kstream.internals.KTableValueGetter;
import org.apache.kafka.streams.kstream.internals.KTableValueGetterSupplier;
import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;

import com.trivago.streams.reflect.KTableReflect;

    
public class KTableJoinMergeProcessorSupplier<K,V,KL,VL,KR,VR> implements KTableProcessorSupplier<K, V, V>{
	private boolean sendOldValue = false;


	private KTableImpl<KL, ?, VL> leftAccessor;
	private RangeKeyValueGetterProviderAndProcessorSupplier<K, V, KL, VL, VR> rightAccessor;
	private ValueMapper<K, KL>leftKeyExtractor;
	private ValueJoiner<VL, VR, V> joiner;

    public KTableJoinMergeProcessorSupplier(KTableImpl<KL, ?, VL> joinByRange,
											RangeKeyValueGetterProviderAndProcessorSupplier<K, V, KL, VL, VR> joinThis, 
											ValueMapper<K, KL>leftKeyExtractor, 
											ValueJoiner<VL, VR, V> joiner) {
		this.leftAccessor = joinByRange;
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
                    context().forward(key, value.newValue);
                }
            }
        };
    }

    @Override
    public KTableValueGetterSupplier<K, V> view() {
        final KTableValueGetter<KL, VL> leftGetter =  ((KTableValueGetter<KL, VL>) extractValueGetterSupplier(leftAccessor));
        final KTableRangeValueGetter<K, VR> rightRepartitionedGetter = rightAccessor.view();
        return new KTableValueGetterSupplier<K, V>() {

            @Override
            public KTableValueGetter<K, V> get() {
                // TODO Auto-generated method stub
                return new KTableValueGetter<K, V>() {

                    @Override
                    public void init(ProcessorContext context) {
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
        };
    }

	@Override
	public void enableSendingOldValues() {
		this.sendOldValue = true;
	}
}
	