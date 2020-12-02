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

import static org.junit.jupiter.api.Assertions.*;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.talend.components.workday.datastore.WorkdayDataStore.AuthenticationType;

class WorkdayDataStoreTest {

    @Test
    void serialOK() throws IOException, ClassNotFoundException {
        final WorkdayDataStore ds = new WorkdayDataStore();
        ds.setAuthEndpoint("auth");
        ds.setAuthentication(AuthenticationType.LOGIN);
        ds.setTenantAlias("alias");
        ds.getLoginForm().setLogin("login");
        ds.getLoginForm().setTenantAlias("tenant");
        ds.getLoginForm().setEndpointPattern("endpoint");
        ds.setClientId("clid");
        ds.setClientSecret("secret");

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try (ObjectOutputStream oos = new ObjectOutputStream(out)) {
            oos.writeObject(ds);
        }
        ByteArrayInputStream input = new ByteArrayInputStream(out.toByteArray());
        try (ObjectInputStream ois = new ObjectInputStream(input)) {
            final WorkdayDataStore dsClone = (WorkdayDataStore) ois.readObject();
            Assertions.assertEquals(ds, dsClone);
        }
    }
}