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
Expand the name of the chart.
*/}}
{{- define "kafka.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Allow the deployment namespace to be overridden.
*/}}
{{- define "kafka.namespace" -}}
{{ default .Release.Namespace .Values.namespace }}
{{- end }}

{{/*
Create a default fully qualified app name.
We truncate at 63 chars because some Kubernetes name fields are limited to this (by the DNS naming spec).
If release name contains chart name it will be used as a full name.
*/}}
{{- define "kafka.fullname" -}}
{{- if .Values.fullnameOverride }}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- $name := default .Chart.Name .Values.nameOverride }}
{{- if contains $name .Release.Name }}
{{- .Release.Name | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}
{{- end }}

{{- define "kafka.controller" -}}
{{- if .Values.fullnameOverride }}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- $name := default .Chart.Name .Values.nameOverride }}
{{- if contains $name .Release.Name }}
{{- printf "%s-ctl" .Release.Name | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s-ctl" .Release.Name $name | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}
{{- end }}

{{- define "kafka.controller.service" -}}
{{- $name := default .Chart.Name .Values.nameOverride }}
{{- if contains $name .Release.Name }}
{{- "controllers" }}
{{- else }}
{{- printf "%s-controllers" .Release.Name | trunc 63 }}
{{- end }}
{{- end }}

{{- define "kafka.broker.service" -}}
{{- $name := default .Chart.Name .Values.nameOverride }}
{{- if contains $name .Release.Name }}
{{- "brokers" }}
{{- else }}
{{- printf "%s-brokers" .Release.Name | trunc 63 }}
{{- end }}
{{- end }}

{{/*
Create chart name and version as used by the chart label.
*/}}
{{- define "kafka.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels
*/}}
{{- define "kafka.labels" -}}
helm.sh/chart: {{ include "kafka.chart" . }}
{{ include "kafka.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Selector labels
*/}}
{{- define "kafka.selectorLabels" -}}
app.kubernetes.io/name: {{ include "kafka.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
Create the name of the service account to use
*/}}
{{- define "kafka.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "kafka.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.name }}
{{- end }}
{{- end }}

{{/*
Define the image name
*/}}
{{- define "kafka.image" -}}
{{- if .Values.image.registry }}
{{- .Values.image.registry | trimSuffix "/" }}{{ "/" }}
{{- end }}
{{- .Values.image.repository }}{{ ":" }}
{{- default .Chart.AppVersion .Values.image.tag }}
{{- end }}

{{/*
Construct the static controller quorum voters string.
*/}}
{{- define "kafka.controller.quorum.voters" -}}
{{- $ns := include "kafka.namespace" $ -}}
{{- $serviceName := include "kafka.controller.service" . -}}
{{- $baseId := int .Values.controller.baseId }}
{{- range $i,$v := untilStep $baseId (int (add $baseId .Values.controller.replicas)) 1 }}
{{- if gt $i 0 }},{{ end -}}
{{- $v }}@{{ include "kafka.controller" $ }}-{{ $i }}.{{ $serviceName }}.{{ $ns }}.svc.cluster.local:9093
{{- end }}
{{- end }}


{{/*
Construct the static controller quorum voters string.
*/}}
{{- define "kafka.controller.quorum.bootstrap.servers" -}}
{{- $ns := include "kafka.namespace" $ -}}
{{- $serviceName := include "kafka.controller.service" . -}}
{{- $baseId := int .Values.controller.baseId }}
{{- range $i,$v := untilStep $baseId (int (add $baseId .Values.controller.replicas)) 1 }}
{{- if gt $i 0 }},{{ end -}}
{{ include "kafka.controller" $ }}-{{ $i }}.{{ $serviceName }}.{{ $ns }}.svc.cluster.local:9093
{{- end }}
{{- end }}

{{/*
Construct the list of broker URLs to be used for the bootstrap server list.
*/}}
{{- define "kafka.brokerUrls" -}}
{{- $ns := include "kafka.namespace" $ -}}
{{- $serviceName := include "kafka.broker.service" . -}}
{{- range $i,$v := untilStep 1 (int (add 1 .Values.broker.replicas)) 1 }}
{{- if gt $i 0 }},{{ end -}}
{{ include "kafka.fullname" $ }}-{{ $i }}.{{ $serviceName }}.{{ $ns }}.svc.cluster.local:9092
{{- end }}
{{- end }}

{{/*
URL of the first broker.
*/}}
{{- define "kafka.broker0url" -}}
{{- $ns := include "kafka.namespace" $ -}}
{{- $serviceName := include "kafka.broker.service" . -}}
{{ include "kafka.fullname" $ }}-0.{{ $serviceName }}.{{ $ns }}.svc.cluster.local:9092
{{- end }}

{{/*
Initial Controllers list
*/}}
{{- define "kafka.initialControllers" -}}
{{- $ns := include "kafka.namespace" $ -}}
{{- $serviceName := include "kafka.controller.service" . -}}
{{- $baseId := int .Values.controller.baseId }}
{{- $port := int .Values.controller.port }}
{{- range $i,$v := untilStep $baseId (int (add $baseId .Values.controller.replicas)) 1 }}
{{- $dirId := randBytes 16 | trimAll "=" | replace "/" "_" | replace "+" "-" -}}
{{- if gt $i 0 }},{{ end -}}
{{- $v }}@{{ include "kafka.controller" $ }}-{{ $i }}.{{ $serviceName }}.{{ $ns }}.svc.cluster.local:{{ $port }}:{{ $dirId }}
{{- end }}
{{- end }}
