/*
 * Copyright 2010 LinkedIn
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.Constructor;
import java.net.URI;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import org.apache.log4j.Logger;

public class Props extends Properties {

	private static final long serialVersionUID = 1L;
	private static Logger logger = Logger.getLogger(Props.class);
	
	/**
	 * default constructor
	 */
	public Props() {
		super();
	}

	/**
	 * copy constructor 
	 * @param props
	 */
	public Props(Props props) {
		if (props != null) {
			this.put(props);
		}
	}
	
	/**
	 * construct props from a list of files
	 * @param files		paths of files
	 * @throws FileNotFoundException
	 * @throws IOException
	 */
	public Props(String... files) throws FileNotFoundException, IOException {
		this(Arrays.asList(files));
	}

	/**
	 * construct props from a list of files
	 * @param files		paths of files
	 * @throws FileNotFoundException
	 * @throws IOException
	 */
	public Props(List<String> files) throws FileNotFoundException, IOException {

		for (int i = 0; i < files.size(); i++) {
			InputStream input = new BufferedInputStream(new FileInputStream(
					new File(files.get(i)).getAbsolutePath()));
			super.load(input);
			input.close();
		}
	}

	/**
	 * construct props from a list of input streams
	 * @param inputStreams
	 * @throws IOException
	 */
	public Props(InputStream... inputStreams) throws IOException {
		for (InputStream stream : inputStreams)
			super.load(stream);
	}

	/**
	 * construct props from a list of maps
	 * @param props
	 */
	public Props(Map<String, String>... props) {
		for (int i = props.length - 1; i >= 0; i--)
			super.putAll(props[i]);
	}

	/**
	 * construct props from a list of Properties
	 * @param properties
	 */
	public Props(Properties... properties) {
		for (int i = properties.length - 1; i >= 0; i--){
			this.put(properties[i]);
		}
	}

	/**
	 * build props from a list of strings and interprate them as
	 * key, value, key, value,....
	 * 
	 * @param args
	 * @return
	 */
	@SuppressWarnings("unchecked")
	public static Props of(String... args) {
		if (args.length % 2 != 0)
			throw new IllegalArgumentException(
					"Must have an equal number of keys and values.");
		Map<String, String> vals = new HashMap<String, String>(args.length / 2);
		for (int i = 0; i < args.length; i += 2)
			vals.put(args[i], args[i + 1]);
		return new Props(vals);
	}

	/**
	 * Put the given Properties into the Props. 
	 * 
	 * @param properties
	 *            The properties to put
	 * 
	 */
	public void put(Properties properties) {
		for (String propName : properties.stringPropertyNames()) {
			super.put(propName, properties.getProperty(propName));
		}
	}

	/**
	 * get property of "key" and split the value by " ," 
	 * @param key		
	 * @return
	 */
	public List<String> getStringList(String key) {
		return getStringList(key, "\\s*,\\s*");
	}

	/**
	 * get property of "key" and split the value by "sep"
	 * @param key
	 * @param sep
	 * @return
	 */
	public List<String> getStringList(String key, String sep) {
		String val =  super.getProperty(key);
		if (val == null || val.trim().length() == 0)
			return Collections.emptyList();

		if (containsKey(key))
			return Arrays.asList(val.split(sep));
		else
			throw new UndefinedPropertyException("Missing required property '"
					+ key + "'");
	}

	/**
	 * get string list with default value. default delimiter is ","
	 * @param key
	 * @param defaultValue
	 * @return
	 */
	public List<String> getStringList(String key, List<String> defaultValue) {
		if (containsKey(key))
			return getStringList(key);
		else
			return defaultValue;
	}

	/**
	 * get string list with default value
	 * @param key
	 * @param defaultValue
	 * @return
	 */
	public List<String> getStringList(String key, List<String> defaultValue,
			String sep) {
		if (containsKey(key))
			return getStringList(key, sep);
		else
			return defaultValue;
	}

	@SuppressWarnings("unchecked")
	protected <T> T getValue(String key, T defaultValue) 
	throws Exception {
		
		if (containsKey(key)) {
			Object value = super.get(key);
			if (value.getClass().isInstance(defaultValue)) {
				return (T)value;
			} else if (value instanceof String) {
				// call constructor(String) to initialize it
				@SuppressWarnings("rawtypes")
				Constructor ct = defaultValue.getClass().getConstructor(String.class);
				String v = ((String)value).trim();
				Object ret = ct.newInstance(v);
				return (T) ret;
			}
			else throw new UndefinedPropertyException ("Property " + key + 
					": cannot convert value of " + value.getClass().getName() + 
					" to " + defaultValue.getClass().getName());
		}
		else {
			return defaultValue;
		}
	}

	@SuppressWarnings("unchecked")
	protected <T> T getValue(String key, Class<T> mclass) 
	throws Exception {
		
		if (containsKey(key)) {
			Object value = super.get(key);
			if (value.getClass().equals(mclass)) {
				return (T)value;
			} else if (value instanceof String) {
				// call constructor(String) to initialize it
				@SuppressWarnings("rawtypes")
				Constructor ct = mclass.getConstructor(String.class);
				String v = ((String)value).trim();
				Object ret = ct.newInstance(v);
				return (T) ret;
			}
			else throw new UndefinedPropertyException ("Property " + key + 
					": cannot convert value of " + value.getClass().getName() + 
					" to " + mclass.getClass().getName());
		}
		else {
			throw new UndefinedPropertyException ("Missing required property '"
					+ key + "'");
		}
	}

	/**
	 * get boolean value
	 * @param key
	 * @param defaultValue
	 * @return
	 * @throws Exception 	if value is not of type boolean or string
	 */
	public Boolean getBoolean(String key, Boolean defaultValue) 
	throws Exception {
		return getValue (key, defaultValue);
	}

	/**
	 * get boolean value
	 * @param key
	 * @param defaultValue
	 * @return
	 * @throws Exception 	if value is not of type boolean or string or 
	 * 										if value doesn't exist
	 */
	public Boolean getBoolean(String key) throws Exception {
		return getValue (key, Boolean.class);
	}

	/**
	 * get long value
	 * @param key
	 * @param defaultValue
	 * @return
	 * @throws Exception 	if value is not of type long or string
	 */
	public Long getLong(String name, Long defaultValue) 
	throws Exception {
		return getValue(name, defaultValue);
	}

	/**
	 * get long value
	 * @param key
	 * @param defaultValue
	 * @return
	 * @throws Exception 	if value is not of type long or string or 
	 * 										if value doesn't exist
	 */
	public Long getLong(String name) throws Exception  {
		return getValue (name, Long.class);
	}

	/**
	 * get integer value
	 * @param key
	 * @param defaultValue
	 * @return
	 * @throws Exception 	if value is not of type integer or string
	 */
	public Integer getInt(String name, Integer defaultValue) 
	throws Exception  {
		return getValue(name, defaultValue);
	}

	/**
	 * get integer value
	 * @param key
	 * @param defaultValue
	 * @return
	 * @throws Exception 	if value is not of type integer or string or 
	 * 										if value doesn't exist
	 */
	public Integer getInt(String name) throws Exception {
		return getValue (name, Integer.class);
	}

	/**
	 * get double value
	 * @param key
	 * @param defaultValue
	 * @return
	 * @throws Exception 	if value is not of type double or string
	 */
	public Double getDouble(String name, double defaultValue) 
	throws Exception {
		return getValue(name, defaultValue);
	}

	/**
	 * get double value
	 * @param key
	 * @param defaultValue
	 * @return
	 * @throws Exception 	if value is not of type double or string or 
	 * 										if value doesn't exist
	 */
	public double getDouble(String name) throws Exception {
		return getValue(name, Double.class);
	}

	/**
	 * get URI value
	 * @param key
	 * @param defaultValue
	 * @return
	 * @throws Exception 	if value is not of type URI or string 
	 */
	public URI getUri(String name, URI defaultValue) throws Exception {
		return getValue(name, defaultValue);
	}

	/**
	 * get URI value
	 * @param key
	 * @param defaultValue
	 * @return
	 * @throws Exception 	if value is not of type URI or string 
	 */
	public URI getUri(String name, String defaultValue) 
	throws Exception {
		URI defaultV = new URI(defaultValue);
		return getValue(name, defaultV);
	}

	/**
	 * get URI value
	 * @param key
	 * @param defaultValue
	 * @return
	 * @throws Exception 	if value is not of type URI or string or 
	 * 										if value doesn't exist
	 */
	public URI getUri(String name) throws Exception {
		return getValue(name, URI.class);
	}

	/**
	 * compare two props 
	 * @param p
	 * @return
	 */
	public boolean equalsProps(Props p) {
		if (p == null) {
			return false;
		}

		final Set<String> myKeySet = getKeySet();
		for (String s : myKeySet) {
			if (!get(s).equals(p.get(s))) {
				return false;
			}
		}

		return myKeySet.size() == p.getKeySet().size();
	}


	/**
	 * Get a map of all properties by string prefix
	 * 
	 * @param prefix
	 *            The string prefix
	 */
	public Map<String, String> getMapByPrefix(String prefix) {
		Map<String, String> values = new HashMap<String, String>();

		for (String key : super.stringPropertyNames()) {
			if (key.startsWith(prefix)) {
				values.put(key.substring(prefix.length()), super.getProperty(key));
			}
		}
		return values;
	}

    /**
     * Store all properties
     * 
     * @param out The stream to write to
     * @throws IOException If there is an error writing
     */
    public void store(OutputStream out) throws IOException {
           super.store(out, null);
    }
    
    /**
     * get all property names
     * @return
     */
	public Set<String> getKeySet() {
		return super.stringPropertyNames();
	}

	/**
	 * log properties
	 * @param comment
	 */
	public void logProperties(String comment) {
		logger.info(comment);

		for (String key : getKeySet()) {
			logger.info("  key=" + key + " value=" + get(key));
		}
	}

	/**
	 * clone a Props
	 * @param p
	 * @return
	 */
	public static Props clone(Props p) {
		return new Props(p);
	}


}
