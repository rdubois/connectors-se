/*
 * Copyright (C) 2006-2021 Talend Inc. - www.talend.com
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
package org.talend.components.jdbc.migration;

import org.talend.sdk.component.api.component.MigrationHandler;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class ConnectionMigrationHandler implements MigrationHandler {

    @Override
    public Map<String, String> migrate(final int incomingVersion, final Map<String, String> incomingData) {
        Map<String, String> migrated = new HashMap<>(incomingData);

        if (incomingVersion < 3) {
            to_3(migrated);
        }

        return migrated;
    }

    private void to_3(final Map<String, String> incomingData) {
        final String jdbcUrl = getOrException(incomingData, "configuration.dataset.connection.jdbcUrl", true);

        incomingData.put("configuration.dataset.connection.jdbcUrl.port", "0");
        incomingData.put("configuration.dataset.connection.jdbcUrl.setRawUrl", "true");
        incomingData.put("configuration.dataset.connection.jdbcUrl.defineProtocol", "false");
        incomingData.put("configuration.dataset.connection.jdbcUrl.rawUrl", jdbcUrl);
    }

    private String getOrException(final Map<String, String> incomingData, final String key, final boolean deleteKey) {
        final String v = Optional.ofNullable(incomingData.get(key)).orElseThrow(
                () -> new RuntimeException(String.format("Given key doesn't exists in configuration to migrate : %s", key)));

        if (deleteKey) {
            incomingData.remove(key);
        }

        return v;
    }

}
