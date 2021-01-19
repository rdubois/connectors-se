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

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class ConnectionMigrationHandlerTest {

    @Test
    public void to_3() {
        final ConnectionMigrationHandler connectionMigrationHandler = new ConnectionMigrationHandler();

        final String url = "jdbc.test.driver://192.168.0.1/mydb?key=val";
        final String oldKey = "configuration.dataset.connection.jdbcUrl";

        Map<String, String> conf = new HashMap<>();
        conf.put("configuration.createTableIfNotExists", "false");
        conf.put("configuration.varcharLength", "255");
        conf.put("configuration.$actionOnData_name", "INSERT");
        conf.put("configuration.rewriteBatchedStatements", "true");
        conf.put("configuration.sortStrategy", "COMPOUND");
        conf.put("configuration.distributionStrategy", "AUTO");
        conf.put("configuration.dataset.connection.connectionValidationTimeOut", "10");
        conf.put("configuration.dataset.connection.connectionTimeOut", "30");
        conf.put("configuration.dataset.connection.dbType", "Snowflake");
        conf.put("configuration.dataset.connection.$dbType_name", "Snowflake");
        conf.put(oldKey, url);
        conf.put("configuration.dataset.connection.userId", "mmm");
        conf.put("configuration.dataset.connection.password", "ssss");
        conf.put("configuration.dataset.advancedCommon.fetchSize", "1000");
        conf.put("configuration.dataset.tableName", "dddd");
        conf.put("configuration.$maxBatchSize", "1000");
        conf.put("configuration.actionOnData", "INSERT");

        final Map<String, String> migrated = connectionMigrationHandler.migrate(1, conf);
        assertFalse(migrated.containsKey(oldKey));
        assertEquals(migrated.get("configuration.dataset.connection.jdbcUrl.rawUrl"), url);
        assertEquals(migrated.get("configuration.dataset.connection.jdbcUrl.port"), "0");
        assertEquals(migrated.get("configuration.dataset.connection.jdbcUrl.setRawUrl"), "true");
        assertEquals(migrated.get("configuration.dataset.connection.jdbcUrl.defineProtocol"), "false");
    }

}