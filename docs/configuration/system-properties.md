---
title: System Properties
description: System Properties
weight: 10
tags: ['kafka', 'docs']
aliases: 
keywords: 
type: docs
---

<!--
 Licensed to the Apache Software Foundation (ASF) under one or more
 contributor license agreements.  See the NOTICE file distributed with
 this work for additional information regarding copyright ownership.
 The ASF licenses this file to You under the Apache License, Version 2.0
 (the "License"); you may not use this file except in compliance with
 the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
-->


Kafka supports some configuration that can be enabled through Java system properties. System properties are usually set by passing the -D flag to the Java virtual machine in which Kafka components are running. Below are the supported system properties. 

  * #### org.apache.kafka.sasl.oauthbearer.allowed.files

This system property is used to determine which files, if any, are allowed to be read by the SASL OAUTHBEARER plugin. This property accepts comma-separated list of files. By default the value is an empty list. 

If users want to enable some files, users need to explicitly set the system property like below. 
        
        -Dorg.apache.kafka.sasl.oauthbearer.allowed.files=/tmp/token,/tmp/private_key.pem  
  
<table>  
<tr>  
<th>

Since:
</th>  
<td>

4.1.0
</td></tr>  
<tr>  
<th>

Default Value:
</th>  
<td>


</td></tr> </table>
  * #### org.apache.kafka.sasl.oauthbearer.allowed.urls

This system property is used to set the allowed URLs as SASL OAUTHBEARER token or jwks endpoints. This property accepts comma-separated list of URLs. By default the value is an empty list. 

If users want to enable some URLs, users need to explicitly set the system property like below. 
        
        -Dorg.apache.kafka.sasl.oauthbearer.allowed.urls=https://www.example.com,file:///tmp/token  
  
<table>  
<tr>  
<th>

Since:
</th>  
<td>

4.0.0
</td></tr>  
<tr>  
<th>

Default Value:
</th>  
<td>


</td></tr> </table>
  * #### org.apache.kafka.disallowed.login.modules

This system property is used to disable the problematic login modules usage in SASL JAAS configuration. This property accepts comma-separated list of loginModule names. By default **com.sun.security.auth.module.JndiLoginModule** loginModule is disabled. 

If users want to enable JndiLoginModule, users need to explicitly reset the system property like below. We advise the users to validate configurations and only allow trusted JNDI configurations. For more details [CVE-2023-25194](/community/cve-list/#CVE-2023-25194). 
        
        -Dorg.apache.kafka.disallowed.login.modules=

To disable more loginModules, update the system property with comma-separated loginModule names. Make sure to explicitly add **JndiLoginModule** module name to the comma-separated list like below. 
        
        -Dorg.apache.kafka.disallowed.login.modules=com.sun.security.auth.module.JndiLoginModule,com.ibm.security.auth.module.LdapLoginModule,com.ibm.security.auth.module.Krb5LoginModule  
  
<table>  
<tr>  
<th>

Since:
</th>  
<td>

3.4.0
</td></tr>  
<tr>  
<th>

Default Value:
</th>  
<td>

com.sun.security.auth.module.JndiLoginModule
</td></tr> </table>
  * #### org.apache.kafka.automatic.config.providers

This system property controls the automatic loading of ConfigProvider implementations in Apache Kafka. ConfigProviders are used to dynamically supply configuration values from sources such as files, directories, or environment variables. This property accepts a comma-separated list of ConfigProvider names. By default, all built-in ConfigProviders are enabled, including **FileConfigProvider** , **DirectoryConfigProvider** , and **EnvVarConfigProvider**.

If users want to disable all automatic ConfigProviders, they need to explicitly set the system property as shown below. Disabling automatic ConfigProviders is recommended in environments where configuration data comes from untrusted sources or where increased security is required. For more details, see [CVE-2024-31141](/community/cve-list/#CVE-2024-31141).
        
        -Dorg.apache.kafka.automatic.config.providers=none

To allow specific ConfigProviders, update the system property with a comma-separated list of fully qualified ConfigProvider class names. For example, to enable only the **EnvVarConfigProvider** , set the property as follows:
        
        -Dorg.apache.kafka.automatic.config.providers=org.apache.kafka.common.config.provider.EnvVarConfigProvider

To use multiple ConfigProviders, include their names in a comma-separated list as shown below:
        
        -Dorg.apache.kafka.automatic.config.providers=org.apache.kafka.common.config.provider.FileConfigProvider,org.apache.kafka.common.config.provider.EnvVarConfigProvider  
  
<table>  
<tr>  
<th>

Since:
</th>  
<td>

3.8.0
</td></tr>  
<tr>  
<th>

Default Value:
</th>  
<td>

All built-in ConfigProviders are enabled
</td></tr> </table>


