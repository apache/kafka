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


import java.net.URI;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.lib.MultipleOutputs;

@SuppressWarnings("deprecation")
public class KafkaETLJob {
    
    public static final String HADOOP_PREFIX = "hadoop-conf.";
    /**
     * Create a job configuration
     */
    @SuppressWarnings("rawtypes")
    public static JobConf createJobConf(String name, String topic, Props props, Class classobj) 
    throws Exception {
        JobConf conf = getJobConf(name, props, classobj);
        
        conf.set("topic", topic);
        
        // input format
        conf.setInputFormat(KafkaETLInputFormat.class);

        //turn off mapper speculative execution
        conf.setMapSpeculativeExecution(false);
        
        // setup multiple outputs
        MultipleOutputs.addMultiNamedOutput(conf, "offsets", SequenceFileOutputFormat.class, 
                    KafkaETLKey.class, BytesWritable.class);


        return conf;
    }
    
    /**
     * Helper function to initialize a job configuration
     */
    public static JobConf getJobConf(String name, Props props, Class classobj) throws Exception {
        JobConf conf = new JobConf();
        // set custom class loader with custom find resource strategy.

        conf.setJobName(name);
        String hadoop_ugi = props.getProperty("hadoop.job.ugi", null);
        if (hadoop_ugi != null) {
            conf.set("hadoop.job.ugi", hadoop_ugi);
        }

        if (props.getBoolean("is.local", false)) {
            conf.set("mapred.job.tracker", "local");
            conf.set("fs.default.name", "file:///");
            conf.set("mapred.local.dir", "/tmp/map-red");

            info("Running locally, no hadoop jar set.");
        } else {
            setClassLoaderAndJar(conf, classobj);
            info("Setting hadoop jar file for class:" + classobj + "  to " + conf.getJar());
            info("*************************************************************************");
            info("          Running on Real Hadoop Cluster(" + conf.get("mapred.job.tracker") + ")           ");
            info("*************************************************************************");
        }

        // set JVM options if present
        if (props.containsKey("mapred.child.java.opts")) {
            conf.set("mapred.child.java.opts", props.getProperty("mapred.child.java.opts"));
            info("mapred.child.java.opts set to " + props.getProperty("mapred.child.java.opts"));
        }

        // Adds External jars to hadoop classpath
        String externalJarList = props.getProperty("hadoop.external.jarFiles", null);
        if (externalJarList != null) {
            String[] jarFiles = externalJarList.split(",");
            for (String jarFile : jarFiles) {
                info("Adding extenral jar File:" + jarFile);
                DistributedCache.addFileToClassPath(new Path(jarFile), conf);
            }
        }

        // Adds distributed cache files
        String cacheFileList = props.getProperty("hadoop.cache.files", null);
        if (cacheFileList != null) {
            String[] cacheFiles = cacheFileList.split(",");
            for (String cacheFile : cacheFiles) {
                info("Adding Distributed Cache File:" + cacheFile);
                DistributedCache.addCacheFile(new URI(cacheFile), conf);
            }
        }

        // Adds distributed cache files
        String archiveFileList = props.getProperty("hadoop.cache.archives", null);
        if (archiveFileList != null) {
            String[] archiveFiles = archiveFileList.split(",");
            for (String archiveFile : archiveFiles) {
                info("Adding Distributed Cache Archive File:" + archiveFile);
                DistributedCache.addCacheArchive(new URI(archiveFile), conf);
            }
        }

        String hadoopCacheJarDir = props.getProperty("hdfs.default.classpath.dir", null);
        if (hadoopCacheJarDir != null) {
            FileSystem fs = FileSystem.get(conf);
            if (fs != null) {
                FileStatus[] status = fs.listStatus(new Path(hadoopCacheJarDir));

                if (status != null) {
                    for (int i = 0; i < status.length; ++i) {
                        if (!status[i].isDir()) {
                            Path path = new Path(hadoopCacheJarDir, status[i].getPath().getName());
                            info("Adding Jar to Distributed Cache Archive File:" + path);

                            DistributedCache.addFileToClassPath(path, conf);
                        }
                    }
                } else {
                    info("hdfs.default.classpath.dir " + hadoopCacheJarDir + " is empty.");
                }
            } else {
                info("hdfs.default.classpath.dir " + hadoopCacheJarDir + " filesystem doesn't exist");
            }
        }

        // May want to add this to HadoopUtils, but will await refactoring
        for (String key : props.stringPropertyNames()) {
            String lowerCase = key.toLowerCase();
            if (lowerCase.startsWith(HADOOP_PREFIX)) {
                String newKey = key.substring(HADOOP_PREFIX.length());
                conf.set(newKey, props.getProperty(key));
            }
        }

        KafkaETLUtils.setPropsInJob(conf, props);
        
        return conf;
    }

    public static void info(String message) {
        System.out.println(message);
    }

    public static void setClassLoaderAndJar(JobConf conf,
            @SuppressWarnings("rawtypes") Class jobClass) {
        conf.setClassLoader(Thread.currentThread().getContextClassLoader());
        String jar = KafkaETLUtils.findContainingJar(jobClass, Thread
                .currentThread().getContextClassLoader());
        if (jar != null) {
            conf.setJar(jar);
        }
    }

}
