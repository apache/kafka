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
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/signal"
	pt "path"
	"regexp"
	"sort"
	"strings"
	"text/template"

	"github.com/spf13/cobra"
	"golang.org/x/exp/slices"
	"golang.org/x/sys/unix"
)

type ConfigSpec struct {
	Prefixes          map[string]bool   `json:"prefixes"`
	Excludes          []string          `json:"excludes"`
	Renamed           map[string]string `json:"renamed"`
	Defaults          map[string]string `json:"defaults"`
	ExcludeWithPrefix string            `json:"excludeWithPrefix"`
}

var (
	re = regexp.MustCompile("[^_]_[^_]")

	ensureCmd = &cobra.Command{
		Use:   "ensure <environment-variable>",
		Short: "checks if environment variable is set or not",
		Args:  cobra.ExactArgs(1),
		RunE:  runEnsureCmd,
	}

	pathCmd = &cobra.Command{
		Use:   "path <path-to-file> <operation>",
		Short: "checks if an operation is permitted on a file",
		Args:  cobra.ExactArgs(2),
		RunE:  runPathCmd,
	}

	renderTemplateCmd = &cobra.Command{
		Use:   "render-template <path-to-template>",
		Short: "renders template to stdout",
		Args:  cobra.ExactArgs(1),
		RunE:  runRenderTemplateCmd,
	}

	renderPropertiesCmd = &cobra.Command{
		Use:   "render-properties <path-to-config-spec>",
		Short: "creates and renders properties to stdout using the json config spec.",
		Args:  cobra.ExactArgs(1),
		RunE:  runRenderPropertiesCmd,
	}
)

func ensure(envVar string) bool {
	_, found := os.LookupEnv(envVar)
	return found
}

func path(filePath string, operation string) (bool, error) {
	switch operation {

	case "readable":
		err := unix.Access(filePath, unix.R_OK)
		if err != nil {
			return false, err
		}
		return true, nil
	case "executable":
		info, err := os.Stat(filePath)
		if err != nil {
			err = fmt.Errorf("error checking executable status of file %q: %w", filePath, err)
			return false, err
		}
		return info.Mode()&0111 != 0, nil //check whether file is executable by anyone, use 0100 to check for execution rights for owner
	case "existence":
		if _, err := os.Stat(filePath); err != nil {
			if os.IsNotExist(err) {
				return false, nil
			}
			return false, err
		}
		return true, nil
	case "writable":
		err := unix.Access(filePath, unix.W_OK)
		if err != nil {
			return false, err
		}
		return true, nil
	default:
		err := fmt.Errorf("unknown operation %q", operation)
		return false, err
	}
}

func renderTemplate(templateFilePath string) error {
	funcs := template.FuncMap{
		"getEnv":             getEnvOrDefault,
		"splitToMapDefaults": splitToMapDefaults,
	}
	t, err := template.New(pt.Base(templateFilePath)).Funcs(funcs).ParseFiles(templateFilePath)
	if err != nil {
		err = fmt.Errorf("error  %q: %w", templateFilePath, err)
		return err
	}
	return buildTemplate(os.Stdout, *t)
}

func buildTemplate(writer io.Writer, template template.Template) error {
	err := template.Execute(writer, GetEnvironment())
	if err != nil {
		err = fmt.Errorf("error building template file : %w", err)
		return err
	}
	return nil
}

func renderConfig(writer io.Writer, configSpec ConfigSpec) error {
	return writeConfig(writer, buildProperties(configSpec, GetEnvironment()))
}

// ConvertKey Converts an environment variable name to a property-name according to the following rules:
// - a single underscore (_) is replaced with a .
// - a double underscore (__) is replaced with a single underscore
// - a triple underscore (___) is replaced with a dash
// Moreover, the whole string is converted to lower-case.
// The behavior of sequences of four or more underscores is undefined.
func ConvertKey(key string) string {
	singleReplaced := re.ReplaceAllStringFunc(key, replaceUnderscores)
	singleTripleReplaced := strings.ReplaceAll(singleReplaced, "___", "-")
	return strings.ToLower(strings.ReplaceAll(singleTripleReplaced, "__", "_"))
}

// replaceUnderscores replaces every underscore '_' by a dot '.'
func replaceUnderscores(s string) string {
	return strings.ReplaceAll(s, "_", ".")
}

// ListToMap splits each and entry of the kvList argument at '=' into a key/value pair and returns a map of all the k/v pair thus obtained.
// this method will only consider values in the list formatted as key=value
func ListToMap(kvList []string) map[string]string {
	m := make(map[string]string, len(kvList))
	for _, l := range kvList {
		parts := strings.Split(l, "=")
		if len(parts) == 2 {
			m[parts[0]] = parts[1]
		}
	}
	return m
}

func splitToMapDefaults(separator string, defaultValues string, value string) map[string]string {
	values := KvStringToMap(defaultValues, separator)
	for k, v := range KvStringToMap(value, separator) {
		values[k] = v
	}
	return values
}

func KvStringToMap(kvString string, sep string) map[string]string {
	return ListToMap(strings.Split(kvString, sep))
}

// GetEnvironment returns the current environment as a map.
func GetEnvironment() map[string]string {
	return ListToMap(os.Environ())
}

// buildProperties creates a map suitable to be output as Java properties from a ConfigSpec and a map representing an environment.
func buildProperties(spec ConfigSpec, environment map[string]string) map[string]string {
	config := make(map[string]string)
	for key, value := range spec.Defaults {
		config[key] = value
	}

	for envKey, envValue := range environment {
		if newKey, found := spec.Renamed[envKey]; found {
			config[newKey] = envValue
		} else {
			if !slices.Contains(spec.Excludes, envKey) && !(len(spec.ExcludeWithPrefix) > 0 && strings.HasPrefix(envKey, spec.ExcludeWithPrefix)) {
				for prefix, keep := range spec.Prefixes {
					if strings.HasPrefix(envKey, prefix) {
						var effectiveKey string
						if keep {
							effectiveKey = envKey
						} else {
							effectiveKey = envKey[len(prefix)+1:]
						}
						config[ConvertKey(effectiveKey)] = envValue
					}
				}
			}
		}
	}
	return config
}

func writeConfig(writer io.Writer, config map[string]string) error {
	// Go randomizes iterations over map by design. We sort properties by name to ease debugging:
	sortedNames := make([]string, 0, len(config))
	for name := range config {
		sortedNames = append(sortedNames, name)
	}
	sort.Strings(sortedNames)
	for _, n := range sortedNames {
		_, err := fmt.Fprintf(writer, "%s=%s\n", n, config[n])
		if err != nil {
			err = fmt.Errorf("error printing configs: %w", err)
			return err
		}
	}
	return nil
}

func loadConfigSpec(path string) (ConfigSpec, error) {
	var spec ConfigSpec
	bytes, err := os.ReadFile(path)
	if err != nil {
		err = fmt.Errorf("error reading from json file %q : %w", path, err)
		return spec, err
	}

	errParse := json.Unmarshal(bytes, &spec)
	if errParse != nil {
		err = fmt.Errorf("error parsing json file %q : %w", path, errParse)
		return spec, err
	}
	return spec, nil
}

func getEnvOrDefault(envVar string, defaultValue string) string {
	val := os.Getenv(envVar)
	if len(val) == 0 {
		return defaultValue
	}
	return val
}

func runEnsureCmd(_ *cobra.Command, args []string) error {
	success := ensure(args[0])
	if !success {
		err := fmt.Errorf("environment variable %q is not set", args[0])
		return err
	}
	return nil
}

func runPathCmd(_ *cobra.Command, args []string) error {
	success, err := path(args[0], args[1])
	if err != nil {
		err = fmt.Errorf("error in checking operation %q on file %q: %w", args[1], args[0], err)
		return err
	}
	if !success {
		err = fmt.Errorf("operation %q on file %q is unsuccessful", args[1], args[0])
		return err
	}
	return nil
}

func runRenderTemplateCmd(_ *cobra.Command, args []string) error {
	err := renderTemplate(args[0])
	if err != nil {
		err = fmt.Errorf("error in rendering template %q: %w", args[0], err)
		return err
	}
	return nil
}

func runRenderPropertiesCmd(_ *cobra.Command, args []string) error {
	configSpec, err := loadConfigSpec(args[0])
	if err != nil {
		err = fmt.Errorf("error in loading config from file %q: %w", args[0], err)
		return err
	}
	err = renderConfig(os.Stdout, configSpec)
	if err != nil {
		err = fmt.Errorf("error in building properties from file %q: %w", args[0], err)
		return err
	}
	return nil
}

func main() {
	rootCmd := &cobra.Command{
		Use:   "ub",
		Short: "utility commands for cp docker images",
		Run:   func(cmd *cobra.Command, args []string) {},
	}

	rootCmd.AddCommand(pathCmd)
	rootCmd.AddCommand(ensureCmd)
	rootCmd.AddCommand(renderTemplateCmd)
	rootCmd.AddCommand(renderPropertiesCmd)

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()
	if err := rootCmd.ExecuteContext(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "error in executing the command: %s", err)
		os.Exit(1)
	}
}
