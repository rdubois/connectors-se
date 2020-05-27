/*
 * Copyright (C) 2006-2020 Talend Inc. - www.talend.com
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package org.talend.components.workday.input;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

import org.talend.components.common.collections.KeyValue;
import org.talend.sdk.component.api.component.MigrationHandler;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class Migration implements MigrationHandler {

    private static final Map<String, String> keyChangeV1toV2 = new HashMap<>();

    static {
        // as migration is mainly change key with same values, we define this map as old key to new key
        keyChangeV1toV2.put("configuration.dataSet.datastore.authEndpoint",
                "configuration.dataSet.datastore.clientIdForm.authEndpoint");
        keyChangeV1toV2.put("configuration.dataSet.datastore.clientId", "configuration.dataSet.datastore.clientIdForm.clientId");
        keyChangeV1toV2.put("configuration.dataSet.datastore.clientSecret",
                "configuration.dataSet.datastore.clientIdForm.clientSecret");
        keyChangeV1toV2.put("configuration.dataSet.datastore.endpoint", "configuration.dataSet.datastore.clientIdForm.endpoint");
        keyChangeV1toV2.put("configuration.dataSet.datastore.tenantAlias",
                "configuration.dataSet.datastore.clientIdForm.tenantAlias");
    }

    @Override
    public Map<String, String> migrate(int incomingVersion, Map<String, String> incomingData) {
        log.info("Starting Workday Producer Migration from " + incomingVersion);

        if (incomingVersion == 1) {
            final Map<String, String> newConfig = incomingData.entrySet() //
                    .stream() //
                    .map(KeyValue::fromEntry) // entrySet to key val
                    .map(KeyValue.keyFunc(this::changeKey)) // apply changeKey to key of KeyVal instances
                    .collect(Collectors.toMap(KeyValue::getKey, KeyValue::getValue));

            newConfig.put("configuration.dataSet.datastore.authentication", "CLIENT_ID");
            if (!incomingData.containsKey("configuration.dataSet.mode")) {
                newConfig.put("configuration.dataSet.mode", "WQL"); // if old default value is absent.
            }
            if (!incomingData.containsKey("configuration.dataSet.datastore.loginForm.endpointPattern")) {
                newConfig.put("configuration.dataSet.datastore.loginForm.endpointPattern",
                        "https://${tenant}.workdaysuv.com/ccx/service/customreport2/gms");
            }
            return newConfig;
        }
        return incomingData;
    }

    /**
     * Search new key from old key.
     * 
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

}
