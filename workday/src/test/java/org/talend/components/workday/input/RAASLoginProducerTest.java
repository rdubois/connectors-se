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

import javax.json.JsonObject;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.talend.components.workday.dataset.RAASLayout;
import org.talend.components.workday.dataset.WorkdayDataSet;
import org.talend.components.workday.datastore.WorkdayDataStore;
import org.talend.components.workday.datastore.WorkdayDataStore.AuthenticationType;
import org.talend.components.workday.service.WorkdayReaderService;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.junit.http.junit5.HttpApi;
import org.talend.sdk.component.junit5.WithComponents;

@HttpApi(useSsl = true)
@WithComponents("org.talend.components.workday")
class RAASLoginProducerTest /* extends WorkdayBaseTest */ {

    private WorkdayDataSet dataset;

    private WorkdayConfiguration cfg;

    @Service
    private WorkdayReaderService service;

    @BeforeEach
    void init() {
        cfg = new WorkdayConfiguration();
        dataset = new WorkdayDataSet();
        this.dataset.setDatastore(this.buildDS());
        this.dataset.setMode(WorkdayDataSet.WorkdayMode.RAAS);
        this.dataset.setRaas(new RAASLayout());
        cfg.setDataSet(dataset);
    }

    @Test
    void nextOK() {

        this.dataset.getRaas().setUser("lmcneil");
        this.dataset.getRaas().setReport("BaseCompensation");

        WorkdayProducer producer = new WorkdayProducer(cfg, service);
        JsonObject o = producer.next();
        Assertions.assertNotNull(o);

        JsonObject o2 = producer.next();
        Assertions.assertNotNull(o2);
    }

    private WorkdayDataStore buildDS() {
        WorkdayDataStore ds = new WorkdayDataStore();
        ds.setAuthentication(AuthenticationType.Login);
        ds.getLoginForm().setLogin("lmcneil");
        ds.getLoginForm().setPassword("TLND&WDay1");
        ds.getLoginForm().setTenantAlias("i-0b105ba19af99cff1");
        return ds;
    }

}