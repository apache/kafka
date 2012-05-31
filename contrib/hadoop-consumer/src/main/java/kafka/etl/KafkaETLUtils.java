/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * 
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.etl;


import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.URL;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.List;
import java.util.Properties;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.BytesWritable;

public class KafkaETLUtils {

	public static PathFilter PATH_FILTER = new PathFilter() {
		@Override
		public boolean accept(Path path) {
			return !path.getName().startsWith("_")
					&& !path.getName().startsWith(".");
		}
	};

	
	public static Path getLastPath(Path path, FileSystem fs) throws IOException {

		FileStatus[] statuses = fs.listStatus(path, PATH_FILTER);

		if (statuses.length == 0) {
			return path;
		} else {
			Arrays.sort(statuses);
			return statuses[statuses.length - 1].getPath();
		}
	}

	public static String getFileName(Path path) throws IOException {
		String fullname = path.toUri().toString();
		String[] parts = fullname.split(Path.SEPARATOR);
		if (parts.length < 1)
			throw new IOException("Invalid path " + fullname);
		return parts[parts.length - 1];
	}

	public static List<String> readText(FileSystem fs, String inputFile)
			throws IOException, FileNotFoundException {
		Path path = new Path(inputFile);
		return readText(fs, path);
	}

	public static List<String> readText(FileSystem fs, Path path)
			throws IOException, FileNotFoundException {
		if (!fs.exists(path)) {
			throw new FileNotFoundException("File " + path + " doesn't exist!");
		}
		BufferedReader in = new BufferedReader(new InputStreamReader(
				fs.open(path)));
		List<String> buf = new ArrayList<String>();
		String line = null;

		while ((line = in.readLine()) != null) {
			if (line.trim().length() > 0)
				buf.add(new String(line.trim()));
		}
		in.close();
		return buf;
	}

	public static void writeText(FileSystem fs, Path outPath, String content)
			throws IOException {
		long timestamp = System.currentTimeMillis();
		String localFile = "/tmp/KafkaETL_tmp_" + timestamp;
		PrintWriter writer = new PrintWriter(new FileWriter(localFile));
		writer.println(content);
		writer.close();

		Path src = new Path(localFile);
		fs.moveFromLocalFile(src, outPath);
	}

	public static Props getPropsFromJob(Configuration conf) {
		String propsString = conf.get("kafka.etl.props");
		if (propsString == null)
			throw new UndefinedPropertyException(
					"The required property kafka.etl.props was not found in the Configuration.");
		try {
			ByteArrayInputStream input = new ByteArrayInputStream(
					propsString.getBytes("UTF-8"));
			Properties properties = new Properties();
			properties.load(input);
			return new Props(properties);
		} catch (IOException e) {
			throw new RuntimeException("This is not possible!", e);
		}
	}

	 public static void setPropsInJob(Configuration conf, Props props)
	  {
	    ByteArrayOutputStream output = new ByteArrayOutputStream();
	    try
	    {
	      props.store(output);
	      conf.set("kafka.etl.props", new String(output.toByteArray(), "UTF-8"));
	    }
	    catch (IOException e)
	    {
	      throw new RuntimeException("This is not possible!", e);
	    }
	  }
	 
	public static Props readProps(String file) throws IOException {
		Path path = new Path(file);
		FileSystem fs = path.getFileSystem(new Configuration());
		if (fs.exists(path)) {
			InputStream input = fs.open(path);
			try {
				// wrap it up in another layer so that the user can override
				// properties
				Props p = new Props(input);
				return new Props(p);
			} finally {
				input.close();
			}
		} else {
			return new Props();
		}
	}

	public static String findContainingJar(
			@SuppressWarnings("rawtypes") Class my_class, ClassLoader loader) {
		String class_file = my_class.getName().replaceAll("\\.", "/")
				+ ".class";
		return findContainingJar(class_file, loader);
	}

	public static String findContainingJar(String fileName, ClassLoader loader) {
		try {
			for (@SuppressWarnings("rawtypes")
			Enumeration itr = loader.getResources(fileName); itr
					.hasMoreElements();) {
				URL url = (URL) itr.nextElement();
				// logger.info("findContainingJar finds url:" + url);
				if ("jar".equals(url.getProtocol())) {
					String toReturn = url.getPath();
					if (toReturn.startsWith("file:")) {
						toReturn = toReturn.substring("file:".length());
					}
					toReturn = URLDecoder.decode(toReturn, "UTF-8");
					return toReturn.replaceAll("!.*$", "");
				}
			}
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		return null;
	}

    public static byte[] getBytes(BytesWritable val) {
        
        byte[] buffer = val.getBytes();
        
        /* FIXME: remove the following part once the below gira is fixed
         * https://issues.apache.org/jira/browse/HADOOP-6298
         */
        long len = val.getLength();
        byte [] bytes = buffer;
        if (len < buffer.length) {
            bytes = new byte[(int) len];
            System.arraycopy(buffer, 0, bytes, 0, (int)len);
        }
        
        return bytes;
    }

}
