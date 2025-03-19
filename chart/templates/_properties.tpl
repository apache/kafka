{{/*
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
*/ -}}

{{/*
The Apache licence header
*/}}
{{- define "kafka.apache.licence" -}}
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

{{- end }}


{{/*
============================================================
format a comma and space delimited list from an array.
============================================================
*/}}
{{- define "kafka.comma-space-list" }}
{{- if kindIs "slice" . }}
{{- printf "%v" (first .) }}
{{- range (rest .) }}
{{- printf ", %v" . }}
{{- end }}
{{- end }}
{{- end }}

{{/*
============================================================
format a comma delimited list from an array.
============================================================
*/}}
{{- define "kafka.comma-list" }}
{{- if kindIs "slice" . }}
{{- printf "%v" (first .) }}
{{- range (rest .) }}
{{- printf ",%v" . }}
{{- end }}
{{- end }}
{{- end }}


{{/*
============================================================
Format the property value, particularly integers and lists
============================================================
*/}}
{{- define "kafka.property.value" }}
{{- if not (kindIs "map" . ) }}
{{- if and (kindIs "float64" . ) (eq (floor .) . ) }}
{{- printf "%d" (int .) }}
{{- else if kindIs "slice" . }}
{{- include "kafka.comma-list" . }}
{{- else }}
{{- printf "%v" . }}
{{- end }}
{{- end }}
{{- end }}




{{/*
============================================================
Combine property values and output in properties format.
============================================================
*/}}
{{- define "kafka.properties" }}
{{- /* Input should be an array of objects to be merged */ -}}
{{- if kindIs "slice" . }}
{{- /* First we need to flatten the objects */ -}}
{{- $flattened := include "kafka.properties.flatten" (first .) | fromYaml }}
{{- range (rest .) }}
{{- $flattened = merge (include "kafka.properties.flatten" . | fromYaml) $flattened }}
{{- end }}
{{- /* Now write out the values as properties */ -}}
{{- range $key, $val := $flattened }}
{{- printf "%s=%s" $key (include "kafka.property.value" $val) | nindent 0 }}
{{- end }}
{{- end }}
{{- end }}


{{/*
============================================================
Flatten a Yaml structure.
If properties are set on the Helm command line with --set
for example --set properties.log.retention.hours=336 then
the Values will be
properties:
  log:
    retention:
      hours: 336
whereas we actually want it to be
properties:
  log.retention.hours: 336
============================================================
Input should be a the properties object
*/}}
{{- define "kafka.properties.flatten" }}
  {{- if kindIs "map" . }}
    {{- /* First just the top level items. These could include items with dots in the name. */ -}}
    {{- $topLevel := include "kafka.properties.flat-yaml" . | fromYaml }}
    {{- /* Now the flattened structured items. */ -}}
    {{- $deepItems := include "kafka.properties.deep-yaml" . | fromYaml }}
    {{- merge $deepItems $topLevel | toYaml | nindent 0 }}
  {{- end }}
{{- end }}

{{- define "kafka.properties.flat-yaml" }}
  {{- range $key, $val := . }}
    {{- if not (kindIs "map" $val) }}
      {{- if kindIs "slice" $val }}
        {{- printf "%s:%s" $key ($val | toYaml | nindent 2) | nindent 0 }}
      {{- else }}
        {{- printf "%s: %s" $key ($val | toYaml) | nindent 0 }}
      {{- end }}
    {{- end }}
  {{- end }}
{{- end }}

{{- define "kafka.properties.deep-yaml" }}
  {{- range $key, $val := . }}
    {{- if kindIs "map" $val }}
      {{- include "kafka.properties.flatten.recurse" (dict "name" $key "props" $val ) }}
    {{- end }}
  {{- end }}
{{- end }}

{{- define "kafka.properties.flatten.recurse" }}
  {{- range $key, $val := $.props }}
    {{- if not (kindIs "map" $val) }}
      {{- printf "%s.%s: %s" $.name $key ($val | toYaml) | nindent 0 }}
    {{- else }}
      {{- include "kafka.properties.flatten.recurse" (dict "name" (printf "%s.%s" $.name $key) "props" $val ) }}
    {{- end }}
  {{- end }}
{{- end }}


{{/*
============================================================
Generate a cluster id, if not specified
The cluster identifier must be a base64 encoded UUID with 
padding removed and '/' replaced by '_' and '+' replaced by '-'.
============================================================
*/}}
{{- define "kafka.clusterid" }}
{{- $configmap := (lookup "v1" "ConfigMap" (include "kafka.namespace" .) (printf "%s-settings" (include "kafka.fullname" .)) ) }}
{{- with $configmap }}
{{- $_ := set $.Values "clusterId" .data.clusterid }}
{{- else }}
{{- if not .Values.clusterId }}
{{- $_ := set $.Values "clusterId" (randBytes 16 | trimAll "=" | replace "/" "_" | replace "+" "-") }}
{{- end }}
{{- end }}
{{- .Values.clusterId }}
{{- end }}


{{/*
============================================================
Calculated properties for the controllers, in Yaml format
============================================================
*/}}
{{- define "kafka.controller.property.values" }}
{{- $ns := include "kafka.namespace" .root }}
{{- $serviceName := include "kafka.controller.service" .root }}
{{- $ctr := include "kafka.controller" .root }}
{{- $nodeId := add (int .root.Values.controller.baseId) .instance }}
process.roles: controller
node.id: {{ $nodeId }}
{{- if .root.Values.controller.staticQuorum }}
controller.quorum.voters: {{ include "kafka.controller.quorum.voters" .root }}
{{- else }}
controller.quorum.bootstrap.servers: {{ include "kafka.controller.quorum.bootstrap.servers" .root }}
{{- end }}
listeners: {{ printf "CONTROLLER://%s-%d.%s.%s.svc.cluster.local:%d" $ctr .instance $serviceName $ns (int .root.Values.controller.port) }}
controller.listener.names: CONTROLLER
log.dirs: /mnt/kafka/controllers/{{ $nodeId }}
{{- end }}


{{/*
============================================================
Calculated properties for the brokers, in Yaml format
============================================================
*/}}
{{- define "kafka.broker.property.values" }}
{{- $ns := include "kafka.namespace" .root }}
{{- $serviceName := include "kafka.broker.service" .root }}
{{- $bkr := include "kafka.fullname" .root }}
{{- $nodeId := add (int .root.Values.broker.baseId) .instance }}
process.roles: broker
node.id: {{ $nodeId }}
{{- if .root.Values.controller.staticQuorum }}
controller.quorum.voters: {{ include "kafka.controller.quorum.voters" .root }}
{{- else }}
controller.quorum.bootstrap.servers: {{ include "kafka.controller.quorum.bootstrap.servers" .root }}
{{- end }}
listeners: {{ printf "PLAINTEXT://%s-%d.%s.%s.svc.cluster.local:%d" $bkr .instance $serviceName $ns (int .root.Values.broker.port) }}
controller.listener.names: CONTROLLER
log.dirs: /mnt/kafka/brokers/{{ $nodeId }}
{{- end }}


{{/*
============================================================
Properties file for the controllers
============================================================
*/}}
{{- define "kafka.controller.properties" }}
{{- $computed := include "kafka.controller.property.values" . | fromYaml }}
{{- include "kafka.apache.licence" .root }}
{{ include "kafka.properties" (list .root.Values.properties .root.Values.controller.properties $computed ) }}
{{ end }}

{{/*
============================================================
Properties file for the brokers
============================================================
*/}}
{{- define "kafka.broker.properties" }}
{{- $computed := include "kafka.broker.property.values" . | fromYaml }}
{{- include "kafka.apache.licence" .root }}
{{ include "kafka.properties" (list .root.Values.properties .root.Values.broker.properties $computed ) }}
{{ end }}


{{/*
============================================================
Properties file for log4j
============================================================
*/}}
{{- define "kafka.log4j2.yaml" }}
{{- include "kafka.apache.licence" . }}

Configuration:
{{- .Values.logging | toYaml | nindent 2 }}

{{- end }}


{{/*
============================================================
Properties file for log4j for tools (i.e. storage tool)
============================================================
*/}}
{{- define "kafka.tools-log4j2.yaml" }}
{{- include "kafka.apache.licence" . }}

Configuration:
{{- .Values.toolLogging | toYaml | nindent 2 }}

{{- end }}


{{/*
============================================================
Script to format the storage for the controllers that
bootstraps all the controllers
============================================================
*/}}
{{- define "kafka.controller.bootstrap.script" }}
${KAFKA_ROOT}/bin/kafka-storage.sh format --cluster-id {{ include "kafka.clusterid" . | quote }} \
    --initial-controllers {{ include "kafka.initialControllers" . | quote }} \
    --config ${KAFKA_ROOT}/config/kafka.properties \
    --feature kraft.version=1 \
    --ignore-formatted $@
{{ end }}