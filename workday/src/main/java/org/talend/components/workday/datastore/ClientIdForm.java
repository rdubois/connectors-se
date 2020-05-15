package org.talend.components.workday.datastore;

import java.io.Serializable;
import java.nio.charset.Charset;
import java.util.Base64;

import org.talend.components.workday.service.UIActionService;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.action.Validable;
import org.talend.sdk.component.api.configuration.ui.DefaultValue;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.configuration.ui.widget.Credential;
import org.talend.sdk.component.api.meta.Documentation;

import lombok.Data;

@Data
@GridLayout({ //
        @GridLayout.Row({ "clientId", "clientSecret" }), //
        @GridLayout.Row({ "tenantAlias" }) //
})
@GridLayout(names = GridLayout.FormType.ADVANCED, //
            value = { @GridLayout.Row("authEndpoint"), //
                      @GridLayout.Row("endpoint") })
public class ClientIdForm implements Serializable {

    private static final long serialVersionUID = -5546051515938154684L;

    @Option
    @Validable(UIActionService.VALIDATION_URL_PROPERTY)
    @DefaultValue("https://auth.api.workday.com")
    @Documentation("Workday token Auth Endpoint (host only, ie: https://auth.api.workday.com/v1/token)")
    private String authEndpoint = "https://auth.api.workday.com";

    @Option
    @Documentation("Workday Client Id")
    private String clientId;

    @Option
    @Credential
    @Documentation("Workday Client Secret")
    private String clientSecret;

    @Option
    @DefaultValue("https://api.workday.com")
    @Documentation("Workday endpoint for REST services")
    private String endpoint = "https://api.workday.com";

    @Option
    @Documentation("Workday tenant alias")
    private String tenantAlias;

    public String getAuthorizationHeader() {
        final String idSecret = this.clientId + ':' + this.clientSecret;
        final String idForHeader = Base64.getEncoder().encodeToString(idSecret.getBytes(Charset.defaultCharset()));
        return "Basic " + idForHeader;
    }
}
