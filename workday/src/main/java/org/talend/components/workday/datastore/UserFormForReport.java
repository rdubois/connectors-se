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

import java.io.Serializable;
import java.nio.charset.Charset;
import java.util.Base64;

import org.talend.components.common.text.Substitutor;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.ui.DefaultValue;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.configuration.ui.widget.Credential;
import org.talend.sdk.component.api.meta.Documentation;

import lombok.Data;

@Data
@GridLayout({ //
        @GridLayout.Row({ "login", "password" }), //
        @GridLayout.Row({ "tenantAlias" }) //
})
@GridLayout(names = GridLayout.FormType.ADVANCED, value = { @GridLayout.Row("endpointPattern") })
@Documentation("Workday connection form with login (for Report As A Service only)")
public class UserFormForReport implements Serializable {

    private static final long serialVersionUID = 4956366610981271937L;

    private static final Substitutor.KeyFinder kf = new Substitutor.KeyFinder("${", "}");

    @Option
    @Documentation("Workday login")
    private String login;

    @Option
    @Credential
    @Documentation("Workday password")
    private String password;

    @Option
    @Documentation("Workday RAAS endpoint pattern (modify with caution)")
    @DefaultValue("https://${tenant}.workdaysuv.com/ccx/service/customreport2/gms")
    private String endpointPattern = "https://${tenant}.workdaysuv.com/ccx/service/customreport2/gms";

    @Option
    @Documentation("Workday tenant alias")
    private String tenantAlias;

    public String getAuthorizationHeader() {
        final String idSecret = this.login + ':' + this.password;
        final String idForHeader = Base64.getEncoder().encodeToString(idSecret.getBytes(Charset.defaultCharset()));
        return "Basic " + idForHeader;
    }

    public String getRealEndpoint() {
        return endpointPattern.replace("${tenant}", this.tenantAlias);
    }
}
