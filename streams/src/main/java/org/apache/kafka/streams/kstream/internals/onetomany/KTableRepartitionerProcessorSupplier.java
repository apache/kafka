package org.apache.kafka.streams.kstream.internals.onetomany;

import org.apache.kafka.streams.kstream.ValueMapper;
import org.apache.kafka.streams.kstream.internals.Change;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.ProcessorSupplier;

public class KTableRepartitionerProcessorSupplier<K, KR, VR> implements ProcessorSupplier<KR, Change<VR>> {
	
	private ValueMapper<VR, K> mapper;
	
	public KTableRepartitionerProcessorSupplier(ValueMapper<VR,K> extractor) {
		this.mapper = extractor;
	}
	
	@Override
	public Processor<KR, Change<VR>> get() {
		return new UnbindChangeProcessor(); 
	}
	
	private class UnbindChangeProcessor implements Processor<KR, Change<VR>>
	{
		private ProcessorContext context;
		
		@Override
		public void init(ProcessorContext context) {
			this.context = context;
		}

		@Override
		public void process(KR key, Change<VR> change) {
			
			if(change.oldValue != null)
			{
				K oldKey = mapper.apply(change.oldValue);
				if(change.newValue != null)
				{
					K newKeyValue = mapper.apply(change.newValue);
					// This is a more tricky story 
					// I only want to be KR to be key of the new partition
					// this wont work as we cant get a grab on the number of
					// partitions of the intermediate topic here
					// therefore the extractor/mapper has to extract the final K here already
					// so we can savely publish a delete and the update 
					// we could skip the delete when we know we are in the same partition
					// and dealing with the same KR and end up in the same partition
					// IF they key equals, the intermediate key will equal which is used
					// to derive the partition
					if(oldKey.equals(newKeyValue))
					{
						context.forward(newKeyValue, change.newValue);
					}
					else  
					{
						context.forward(oldKey, null);
						context.forward(newKeyValue, change.newValue);
					}
				}
				else
				{
					context.forward(oldKey, null);
				}
			}
			else
			{
				if(change.newValue != null)
				{
					K newKeyValue = mapper.apply(change.newValue);
					context.forward(newKeyValue, change.newValue);
				}
				else
				{
					//Both null
				}
			}
		}

		@Override
		public void punctuate(long timestamp) {}

		@Override
		public void close() {}
		
	}


}
