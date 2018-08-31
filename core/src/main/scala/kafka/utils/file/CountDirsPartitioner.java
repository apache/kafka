package kafka.utils.file;

import java.util.*;

/**
 * @author by cry@meitu.com
 * Date: 2018/8/30
 * Description: 
 */
public class CountDirsPartitioner implements DirsPartitioner {


	@Override
	public String partitioner(Map<String, Integer> dirCount) {
		List<Map.Entry<String, Integer>> list = new ArrayList<>(dirCount.entrySet());
		Collections.sort(list, new Comparator<Map.Entry<String, Integer>>() {

			@Override
			public int compare(Map.Entry<String, Integer> o1, Map.Entry<String, Integer> o2) {
				return o1.getValue().compareTo(o2.getValue());
			}

		});
		return list.get(0).getKey();
	}

	@Override
	public String getType() {
		return "count";
	}
}
