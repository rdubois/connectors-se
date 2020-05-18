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
package org.talend.components.workday.service;

import java.net.MalformedURLException;
import java.net.URL;

import org.talend.components.workday.datastore.WorkdayDataStore;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.api.service.asyncvalidation.AsyncValidation;
import org.talend.sdk.component.api.service.asyncvalidation.ValidationResult;
import org.talend.sdk.component.api.service.healthcheck.HealthCheck;
import org.talend.sdk.component.api.service.healthcheck.HealthCheckStatus;

import lombok.extern.slf4j.Slf4j;

import static org.talend.sdk.component.api.service.healthcheck.HealthCheckStatus.Status.KO;
import static org.talend.sdk.component.api.service.healthcheck.HealthCheckStatus.Status.OK;

@Slf4j
@Service
public class UIActionService {

    public static final String HEALTH_CHECK = "WORKDAY_HEALTH_CHECK";

    public static final String VALIDATION_URL_PROPERTY = "WORKDAY_VALIDATION_URL_PROPERTY";

    @Service
    private AccessTokenProvider service;

    @Service
    private I18n i18n;

    @HealthCheck(HEALTH_CHECK)
    public HealthCheckStatus validateConnection(@Option final WorkdayDataStore dataStore) {
        try {
            this.service.getAccessToken(dataStore);
            return new HealthCheckStatus(OK, this.i18n.healthCheckOk());
        } catch (Exception e) {
            return new HealthCheckStatus(KO, this.i18n.healthCheckFailed("msg", e.getMessage()));
        }
    }

    @AsyncValidation(VALIDATION_URL_PROPERTY)
    public ValidationResult validateEndpoint(final String url) {
        try {
            new URL(url);
            return new ValidationResult(ValidationResult.Status.OK, null);
        } catch (MalformedURLException e) {
            return new ValidationResult(ValidationResult.Status.KO, e.getMessage());
        }
    }

}
