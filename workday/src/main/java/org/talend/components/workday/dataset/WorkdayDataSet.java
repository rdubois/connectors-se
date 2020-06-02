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
package org.talend.components.workday.dataset;

import java.io.Serializable;
import java.util.Collections;
import java.util.Map;

import org.talend.components.workday.datastore.WorkdayDataStore;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.condition.ActiveIf;
import org.talend.sdk.component.api.configuration.condition.ActiveIfs;
import org.talend.sdk.component.api.configuration.condition.ActiveIfs.Operator;
import org.talend.sdk.component.api.configuration.type.DataSet;
import org.talend.sdk.component.api.configuration.ui.DefaultValue;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.meta.Documentation;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;

@Data
@DataSet("WorkdayDataSet")
@GridLayout({ @GridLayout.Row("datastore"), //
        @GridLayout.Row({ "mode" }), //
        @GridLayout.Row({ "raas", "wql" }) })
@GridLayout(names = GridLayout.FormType.ADVANCED, value = { @GridLayout.Row("datastore") })
@Documentation("Dataset for workday")
@Slf4j
public class WorkdayDataSet implements Serializable, QueryHelper {

    private static final long serialVersionUID = 5305679660126846088L;

    @Option
    @Documentation("The connection to workday datastore")
    private WorkdayDataStore datastore;

    public enum WorkdayMode {
        WQL("data"),
        RAAS("Report_Entry");

        public final String arrayName;

        WorkdayMode(String arrayName) {
            this.arrayName = arrayName;
        }
    }

    @Option
    @Documentation("Execution mode for workday")
    @ActiveIf(target = "../datastore.authentication", value = "CLIENT_ID")
    @DefaultValue("WQL")
    private WorkdayMode mode = WorkdayMode.WQL;

    @Option
    @Documentation("Layout for report as a service")
    @ActiveIfs(value = { @ActiveIf(target = "../datastore.authentication", value = "LOGIN"),
            @ActiveIf(target = "mode", value = "RAAS") }, operator = Operator.OR)
    private RAASLayout raas;

    @Option
    @Documentation("Layout for workday query language")
    @ActiveIfs(value = { @ActiveIf(target = "../datastore.authentication", value = "CLIENT_ID"),
            @ActiveIf(target = "mode", value = "WQL") }, operator = Operator.AND)
    private WQLLayout wql;

    @Override
    public String getServiceToCall(WorkdayDataStore ds) {
        final QueryHelper helper = this.selectedHelper(ds);
        if (helper == null) {
            log.warn("No service to call for mode {}", this.mode);
            return "";
        }
        return helper.getServiceToCall(ds);
    }

    @Override
    public Map<String, String> extractQueryParam(WorkdayDataStore ds) {
        final QueryHelper helper = this.selectedHelper(ds);
        if (helper == null) {
            log.warn("No query param for mode {}", this.mode);
            return Collections.emptyMap();
        }
        return helper.extractQueryParam(ds);
    }

    private QueryHelper selectedHelper(WorkdayDataStore ds) {
        if (this.mode == WorkdayMode.RAAS) {
            return this.raas;
        }
        if (this.mode == WorkdayMode.WQL) {
            return this.wql;
        }
        return null;
    }
}
