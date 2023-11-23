// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at

//    http://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"os"
	"reflect"
	"testing"
)

func assertEqual(a string, b string, t *testing.T) {
	if a != b {
		t.Error(a + " != " + b)
	}
}

func Test_ensure(t *testing.T) {
	type args struct {
		envVar string
	}
	err := os.Setenv("ENV_VAR", "value")
	if err != nil {
		t.Fatal("Unable to set ENV_VAR for the test")
	}
	tests := []struct {
		name string
		args args
		want bool
	}{
		{
			name: "should exist",
			args: args{
				envVar: "ENV_VAR",
			},
			want: true,
		},
		{
			name: "should not exist",
			args: args{
				envVar: "RANDOM_ENV_VAR",
			},
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := ensure(tt.args.envVar); got != tt.want {
				t.Errorf("ensure() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_path(t *testing.T) {
	type args struct {
		filePath  string
		operation string
	}
	const (
		sampleFile       = "testResources/sampleFile"
		sampleFile2      = "testResources/sampleFile2"
		fileDoesNotExist = "testResources/sampleFile3"
	)
	err := os.Chmod(sampleFile, 0777)
	if err != nil {
		t.Error("Unable to set permissions for the file")
	}
	err = os.Chmod(sampleFile2, 0000)
	if err != nil {
		t.Error("Unable to set permissions for the file")
	}
	tests := []struct {
		name    string
		args    args
		want    bool
		wantErr bool
	}{
		{
			name: "file readable",
			args: args{filePath: sampleFile,
				operation: "readable"},
			want:    true,
			wantErr: false,
		},
		{
			name: "file writable",
			args: args{filePath: sampleFile,
				operation: "writable"},
			want:    true,
			wantErr: false,
		},
		{
			name: "file executable",
			args: args{filePath: sampleFile,
				operation: "executable"},
			want:    true,
			wantErr: false,
		},
		{
			name: "file existence",
			args: args{filePath: sampleFile,
				operation: "existence"},
			want:    true,
			wantErr: false,
		},
		{
			name: "file not readable",
			args: args{filePath: sampleFile2,
				operation: "readable"},
			want:    false,
			wantErr: true,
		},
		{
			name: "file not writable",
			args: args{filePath: sampleFile2,
				operation: "writable"},
			want:    false,
			wantErr: true,
		},
		{
			name: "file not executable",
			args: args{filePath: sampleFile2,
				operation: "executable"},
			want:    false,
			wantErr: false,
		},
		{
			name: "file does not exist",
			args: args{filePath: fileDoesNotExist,
				operation: "existence"},
			want:    false,
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := path(tt.args.filePath, tt.args.operation)
			if (err != nil) != tt.wantErr {
				t.Errorf("path() error = %v, wantErr %v", err, tt.wantErr)
			}
			if got != tt.want {
				t.Errorf("path() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_renderTemplate(t *testing.T) {
	type args struct {
		templateFilePath string
	}
	const (
		fileExistsAndRenderable = "testResources/sampleLog4j.template"
		fileDoesNotExist        = "testResources/RandomFileName"
	)
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name:    "render template success",
			args:    args{templateFilePath: fileExistsAndRenderable},
			wantErr: false,
		},
		{
			name:    "render template failure ",
			args:    args{templateFilePath: fileDoesNotExist},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := renderTemplate(tt.args.templateFilePath); (err != nil) != tt.wantErr {
				t.Errorf("renderTemplate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
func Test_convertKey(t *testing.T) {
	type args struct {
		key string
	}
	tests := []struct {
		name       string
		args       args
		wantString string
	}{
		{
			name:       "Capitals",
			args:       args{key: "KEY"},
			wantString: "key",
		},
		{
			name:       "Capitals with underscore",
			args:       args{key: "KEY_FOO"},
			wantString: "key.foo",
		},
		{
			name:       "Capitals with double underscore",
			args:       args{key: "KEY__UNDERSCORE"},
			wantString: "key_underscore",
		},
		{
			name:       "Capitals with double and single underscore",
			args:       args{key: "KEY_WITH__UNDERSCORE_AND__MORE"},
			wantString: "key.with_underscore.and_more",
		},
		{
			name:       "Capitals with triple underscore",
			args:       args{key: "KEY___DASH"},
			wantString: "key-dash",
		},
		{
			name:       "capitals with double,triple and single underscore",
			args:       args{key: "KEY_WITH___DASH_AND___MORE__UNDERSCORE"},
			wantString: "key.with-dash.and-more_underscore",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if result := ConvertKey(tt.args.key); result != tt.wantString {
				t.Errorf("ConvertKey() result = %v, wantStr %v", result, tt.wantString)
			}
		})
	}
}

func Test_buildProperties(t *testing.T) {
	type args struct {
		spec        ConfigSpec
		environment map[string]string
	}
	tests := []struct {
		name string
		args args
		want map[string]string
	}{
		{
			name: "only defaults",
			args: args{
				spec: ConfigSpec{
					Defaults: map[string]string{
						"default.property.key": "default.property.value",
						"bootstrap.servers":    "unknown",
					},
				},
				environment: map[string]string{
					"PATH":                          "thePath",
					"KAFKA_BOOTSTRAP_SERVERS":       "localhost:9092",
					"KAFKA_IGNORED":                 "ignored",
					"KAFKA_EXCLUDE_PREFIX_PROPERTY": "ignored",
				},
			},
			want: map[string]string{"bootstrap.servers": "unknown", "default.property.key": "default.property.value"},
		},
		{
			name: "server properties",
			args: args{
				spec: ConfigSpec{
					Prefixes: map[string]bool{"KAFKA": false},
					Excludes: []string{"KAFKA_IGNORED"},
					Renamed:  map[string]string{},
					Defaults: map[string]string{
						"default.property.key": "default.property.value",
						"bootstrap.servers":    "unknown",
					},
					ExcludeWithPrefix: "KAFKA_EXCLUDE_PREFIX_",
				},
				environment: map[string]string{
					"PATH":                          "thePath",
					"KAFKA_BOOTSTRAP_SERVERS":       "localhost:9092",
					"KAFKA_IGNORED":                 "ignored",
					"KAFKA_EXCLUDE_PREFIX_PROPERTY": "ignored",
				},
			},
			want: map[string]string{"bootstrap.servers": "localhost:9092", "default.property.key": "default.property.value"},
		},
		{
			name: "kafka properties",
			args: args{
				spec: ConfigSpec{
					Prefixes: map[string]bool{"KAFKA": false},
					Excludes: []string{"KAFKA_IGNORED"},
					Renamed:  map[string]string{},
					Defaults: map[string]string{
						"default.property.key": "default.property.value",
						"bootstrap.servers":    "unknown",
					},
					ExcludeWithPrefix: "KAFKA_EXCLUDE_PREFIX_",
				},
				environment: map[string]string{
					"KAFKA_FOO":                       "foo",
					"KAFKA_FOO_BAR":                   "bar",
					"KAFKA_IGNORED":                   "ignored",
					"KAFKA_WITH__UNDERSCORE":          "with underscore",
					"KAFKA_WITH__UNDERSCORE_AND_MORE": "with underscore and more",
					"KAFKA_WITH___DASH":               "with dash",
					"KAFKA_WITH___DASH_AND_MORE":      "with dash and more",
				},
			},
			want: map[string]string{"bootstrap.servers": "unknown", "default.property.key": "default.property.value", "foo": "foo", "foo.bar": "bar", "with-dash": "with dash", "with-dash.and.more": "with dash and more", "with_underscore": "with underscore", "with_underscore.and.more": "with underscore and more"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := buildProperties(tt.args.spec, tt.args.environment); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("buildProperties() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_splitToMapDefaults(t *testing.T) {
	type args struct {
		separator     string
		defaultValues string
		value         string
	}
	tests := []struct {
		name string
		args args
		want map[string]string
	}{
		{
			name: "split to default",
			args: args{
				separator:     ",",
				defaultValues: "kafka=INFO,kafka.producer.async.DefaultEventHandler=DEBUG,state.change.logger=TRACE",
				value:         "kafka.producer.async.DefaultEventHandler=ERROR,kafka.request.logger=WARN",
			},
			want: map[string]string{"kafka": "INFO", "kafka.producer.async.DefaultEventHandler": "ERROR", "kafka.request.logger": "WARN", "state.change.logger": "TRACE"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := splitToMapDefaults(tt.args.separator, tt.args.defaultValues, tt.args.value); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("splitToMapDefaults() = %v, want %v", got, tt.want)
			}
		})
	}
}
