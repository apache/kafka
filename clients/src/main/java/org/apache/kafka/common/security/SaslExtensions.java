package org.apache.kafka.common.security;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class SaslExtensions {
    protected Map<String, String> extensionMap;

    public SaslExtensions() {
        this(Collections.<String, String>emptyMap());
    }

    public SaslExtensions(String extensions) {
        this(stringToMap(extensions));
    }

    public SaslExtensions(Map<String, String> extensionMap) {
        this.extensionMap = extensionMap;
    }

    public String extensionValue(String name) {
        return extensionMap.get(name);
    }

    public Set<String> extensionNames() {
        return extensionMap.keySet();
    }

    @Override
    public String toString() {
        return mapToString(extensionMap);
    }

    /*
        Converts an extensions string into a Map<String, String>.
        "key=hey,keyTwo=hi,keyThree=hello" => { key: "hey", keyTwo: "hi", keyThree: "hello" }
     */
    protected static Map<String, String> stringToMap(String extensions) {
        Map<String, String> extensionMap = new HashMap<>();

        if (!extensions.isEmpty()) {
            String[] attrvals = extensions.split(",");
            for (String attrval : attrvals) {
                String[] array = attrval.split("=", 2);
                extensionMap.put(array[0], array[1]);
            }
        }
        return extensionMap;
    }

    /*
        Converts a Map class into an extensions string, concatenating keys and values
        Example:
            { key: "hello", keyTwo: "hi" } => "key=hello,keyTwo=hi"
     */
    protected static String mapToString(Map<String, String> extensionMap) {
        ArrayList<String> keyValuePairs = new ArrayList<>();

        for (Map.Entry<String, String> entry : extensionMap.entrySet()) {
            keyValuePairs.add(entry.getKey() + '=' + entry.getValue());
        }

        return String.join(",", keyValuePairs);
    }
}
