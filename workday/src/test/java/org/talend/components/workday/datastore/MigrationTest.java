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
package org.talend.components.workday.datastore;

import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class MigrationTest {

    @Test
    void migrate() {
        final Migration migration = new Migration();

        final Map<String, String> config = new HashMap<>();

        config.put("authEndpoint", "authEndpoint");
        config.put("clientId", "clientId");
        config.put("clientSecret", "clientSecret");
        config.put("endpoint", "endpoint");
        config.put("tenantAlias", "tenantAlias");

        config.put("other", "other");

        final Map<String, String> configV2 = migration.migrate(1, config);
        Assertions.assertEquals(config.size() + 2, configV2.size(), "unexpected size");
        Assertions.assertEquals("authEndpoint", configV2.get("clientIdForm.authEndpoint"));
        Assertions.assertEquals("other", configV2.get("other"));
        Assertions.assertEquals("clientId", configV2.get("clientIdForm.clientId"));
    }
}