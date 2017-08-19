package org.apache.kafka.streams.kstream.internals.onetomany;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.kstream.ValueJoiner;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.apache.kafka.streams.kstream.internals.Change;
import org.apache.kafka.streams.kstream.internals.KTableRangeValueGetter;
import org.apache.kafka.streams.kstream.internals.KTableRangeValueGetterSupplier;
import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.ProcessorSupplier;
import org.apache.kafka.streams.state.KeyValueIterator;

public class KTableKTableRangeJoin<K, V, KL, VL, VR> implements ProcessorSupplier<KL, Change<VL>> {

	private ValueJoiner<VL, VR, V> joiner;
	private KTableRangeValueGetterSupplier<K, VR> right;
	private ValueMapper<KL, K> faker;
	
    public KTableKTableRangeJoin(KTableRangeValueGetterSupplier<K, VR> right, ValueJoiner<VL, VR, V> joiner, ValueMapper<KL, K> faker) {
    	this.right = right;
        this.joiner = joiner;
        this.faker = faker;
    }

	@Override
    public Processor<KL, Change<VL>> get() {
        return new KTableKTableJoinProcessor(right);
    }
	

    private class KTableKTableJoinProcessor extends AbstractProcessor<KL, Change<VL>> {

		private KTableRangeValueGetter<K, VR> rightValueGetter;

        public KTableKTableJoinProcessor(KTableRangeValueGetterSupplier<K, VR> right) {
            this.rightValueGetter = right.get();
        }

        @SuppressWarnings("unchecked")
        @Override
        public void init(ProcessorContext context) {
            super.init(context);
            rightValueGetter.init(context);
        }

        /**
         * @throws StreamsException if key is null
         */
        @Override
        public void process(KL key, Change<VL> leftChange) {
            // the keys should never be null
            if (key == null)
                throw new StreamsException("Record key for KTable join operator should not be null.");

            K prefixKey = faker.apply(key);
            
           final KeyValueIterator<K,VR> rightValues = rightValueGetter.prefixScan(prefixKey);
            
            while(rightValues.hasNext()){
                  KeyValue<K, VR> rightKeyValue = rightValues.next();
                  K realKey = rightKeyValue.key;
                  VR value2 = rightKeyValue.value;
                  V newValue = null;
  				  V oldValue = null;
                  
                  
                  if (leftChange.oldValue != null) {
                	  oldValue = joiner.apply(leftChange.oldValue, value2);
                  }
                  
                  if (leftChange.newValue != null){
                      newValue = joiner.apply(leftChange.newValue, value2);
                  }
            	 
				 context().forward(realKey, new Change<>(newValue, oldValue));
                  
            }
        }
    }
}
