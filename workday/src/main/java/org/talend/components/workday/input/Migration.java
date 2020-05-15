package org.talend.components.workday.input;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.talend.sdk.component.api.component.MigrationHandler;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class Migration implements MigrationHandler {

    private static final Map<String, String> keyChangeV1toV2 = new HashMap<>();

    static {
        // as migration is mainly change key with same values, we define this map as old key to new key
        keyChangeV1toV2.put("configuration.dataSet.datastore.authEndpoint", "configuration.dataSet.datastore.clientIdForm.authEndpoint");
        keyChangeV1toV2.put("configuration.dataSet.datastore.clientId", "configuration.dataSet.datastore.clientIdForm.clientId");
        keyChangeV1toV2.put("configuration.dataSet.datastore.clientSecret", "configuration.dataSet.datastore.clientIdForm.clientSecret");
        keyChangeV1toV2.put("configuration.dataSet.datastore.endpoint", "configuration.dataSet.datastore.clientIdForm.endpoint");
        keyChangeV1toV2.put("configuration.dataSet.datastore.tenantAlias", "configuration.dataSet.datastore.clientIdForm.tenantAlias");
    }

    @Override
    public Map<String, String> migrate(int incomingVersion,
                                       Map<String, String> incomingData) {
        log.info("Starting Workday Producer Migration from " + incomingVersion);
        if (incomingVersion == 1) {
            final Map<String, String> newConfig = incomingData.entrySet() //
                    .stream()  //
                    .map(KeyVal::fromEntry)  // entrySet to key val
                    .map(KeyVal.keyFunc(this::changeKey)) // apply changeKey to key of KeyVal instances
                    .collect(Collectors.toMap(KeyVal::getKey, KeyVal::getValue));

            newConfig.put("config.dataSet.datastore.authentication", "ClientId");
            return newConfig;
        }
        return incomingData;
    }

    /**
     * Search new key from old key.
     * @param key : old key
     * @return new key
     */
    private String changeKey(String key) {
        if (keyChangeV1toV2.containsKey(key)) {
            return keyChangeV1toV2.get(key);
        }
        // if not in old key -> new key map mean key is unchanged.
        return key;
    }

    /**
     * class to manipulate key/value for map.
     */
    private static class KeyVal {
        private final String key;

        private final String value;

        public String getKey() {
            return key;
        }

        public String getValue() {
            return value;
        }

        public KeyVal(String key, String value) {
            this.key = key;
            this.value = value;
        }

        public static KeyVal fromEntry(Map.Entry<String, String> e) {
            return new KeyVal(e.getKey(), e.getValue());
        }

        /**
         * Apply a function on key to get a new KeyVal instance.
         * @param fkey : function to apply on key.
         * @return new KeyVal.
         */
        public KeyVal applyKey(Function<String , String> fkey) {
            final String newKey = fkey.apply(this.key);
            return new KeyVal(newKey, this.value);
        }

        /**
         * Transform key transform function to KeyVal transform function.
         * (Simplify usage in stream)
         * @param fkey : key function.
         * @return keyVal function.
         */
        public static Function<KeyVal, KeyVal> keyFunc(Function<String, String> fkey) {
            return (KeyVal kv) -> kv.applyKey(fkey);
        }
    }

}
