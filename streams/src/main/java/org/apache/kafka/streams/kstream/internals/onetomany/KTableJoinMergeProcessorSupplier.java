package org.apache.kafka.streams.kstream.internals.onetomany;

import org.apache.kafka.streams.kstream.ValueJoiner;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.apache.kafka.streams.kstream.internals.Change;
import org.apache.kafka.streams.kstream.internals.KTableProcessorSupplier;
import org.apache.kafka.streams.kstream.internals.KTableRangeValueGetterSupplier;
import org.apache.kafka.streams.kstream.internals.KTableValueGetter;
import org.apache.kafka.streams.kstream.internals.KTableValueGetterSupplier;
import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;

public class KTableJoinMergeProcessorSupplier<K0,V0,K,V,KO,VO> implements KTableProcessorSupplier<K0, V0, V0>{
	private boolean sendOldValue = false;


	private KTableValueGetterSupplier<K, V> leftValueGetter;
	private KTableValueGetterSupplier<K0, VO> rightValueGetter;
	private ValueMapper<K0, K>leftKeyExtractor;
	private ValueJoiner<V, VO, V0> joiner;

    public KTableJoinMergeProcessorSupplier(KTableValueGetterSupplier<K, V> leftValueGetter,
    										KTableValueGetterSupplier<K0, VO> rightValueGetter, 
											ValueMapper<K0, K>leftKeyExtractor, 
											ValueJoiner<V, VO, V0> joiner) {
		this.leftValueGetter = leftValueGetter;
		this.rightValueGetter = rightValueGetter;
		this.leftKeyExtractor = leftKeyExtractor;
		this.joiner = joiner;
	}


	public KTableJoinMergeProcessorSupplier(
			KTableValueGetterSupplier<K0, V0> valueGetterSupplier,
			KTableRangeValueGetterSupplier<K0, VO> valueGetterSupplier2,
			ValueMapper<K0, K0> leftKeyExtractor2, ValueJoiner<V0, VO, V0> joiner2) {
		// TODO Auto-generated constructor stub
	}


	@Override
    public Processor<K0, Change<V0>> get() {
        return new AbstractProcessor<K0, Change<V0>>() {

            @Override
            public void process(K0 key, Change<V0> value) {
                if (sendOldValue) {
                    context().forward(key, value);
                } else {
                    context().forward(key, new Change<>(value.newValue, null));
                }
            }
        };
    }

    @Override
    public KTableValueGetterSupplier<K0, V0> view() {
        final KTableValueGetter<K, V> leftGetter =  leftValueGetter.get();
        final KTableValueGetter<K0, VO> rightRepartitionedGetter = rightValueGetter.get();
        
        return new KTableValueGetterSupplier<K0, V0>() {

            @Override
            public KTableValueGetter<K0, V0> get() {
                // TODO Auto-generated method stub
                return new KTableValueGetter<K0, V0>() {

                    @Override
                    public void init(ProcessorContext context) {
                    	/* initialize here?
                    	leftGetter.init(context);
                    	rightRepartitionedGetter.init(context);
                    	*/
                    }

                    @Override
                    public V0 get(K0 key) {
                        V leftvalue = leftGetter.get(leftKeyExtractor.apply(key));
                        VO rigthValue = rightRepartitionedGetter.get(key);
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
				String[] leftNames = leftValueGetter.storeNames();
				String[] right = rightValueGetter.storeNames();
				String[] result = new String[leftNames.length + right.length];
				System.arraycopy(leftNames, 0, result, 0, leftNames.length);
				System.arraycopy(right, 0, result, leftNames.length, right.length);
				return result; //no clue about semantics here? that way?
				
			}
        };
    }

	@Override
	public void enableSendingOldValues() {
		this.sendOldValue = true;
	}
}
	