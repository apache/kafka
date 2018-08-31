package kafka.utils.file;

import java.util.Map;

public interface DirsPartitioner {

	String partitioner(Map<String, Integer> dirCount);

	String getType();
}
