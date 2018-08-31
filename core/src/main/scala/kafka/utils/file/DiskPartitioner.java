package kafka.utils.file;

import java.io.File;
import java.util.Map;

/**
 * @author by cry@meitu.com
 * Date: 2018/8/31
 * Description: 
 */
public class DiskPartitioner implements DirsPartitioner {

	@Override
	public String partitioner(Map<String, Integer> dirCount) {
		long template = 0;
		String result = null;
		File file;
		for (String fileName : dirCount.keySet()) {
			file = new File(fileName);
			if (template < file.getFreeSpace()) {
				template = file.getFreeSpace();
				result = fileName;
			}
		}
		return result;
	}

	@Override
	public String getType() {
		return "disk";
	}

}
