package org.talend.components.workday.input;

import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class MigrationTest {

    @Test
    void migrate() {
        final Migration migration = new Migration();

        final Map<String, String> config = new HashMap<>();

        config.put("configuration.dataSet.datastore.authEndpoint", "authEndpoint");
        config.put("configuration.dataSet.datastore.clientId", "clientId");
        config.put("configuration.dataSet.datastore.clientSecret", "clientSecret");
        config.put("configuration.dataSet.datastore.endpoint", "endpoint");
        config.put("configuration.dataSet.datastore.tenantAlias", "tenantAlias");

        config.put("configuration.dataSet.datastore.other", "other");

        final Map<String, String> configV2 = migration.migrate(1, config);
        Assertions.assertEquals(config.size()  + 1, configV2.size(), "unexpected size");
        Assertions.assertEquals("authEndpoint", configV2.get("configuration.dataSet.datastore.clientIdForm.authEndpoint"));
        Assertions.assertEquals("other", configV2.get("configuration.dataSet.datastore.other"));
        Assertions.assertEquals("clientId", configV2.get("configuration.dataSet.datastore.clientIdForm.clientId"));
    }
}