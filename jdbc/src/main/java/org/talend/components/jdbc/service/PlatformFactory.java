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
package org.talend.components.jdbc.service;

import org.talend.components.jdbc.configuration.JdbcConfiguration;
import org.talend.components.jdbc.datastore.JdbcConnection;
import org.talend.components.jdbc.output.platforms.DerbyPlatform;
import org.talend.components.jdbc.output.platforms.MSSQLPlatform;
import org.talend.components.jdbc.output.platforms.MariaDbPlatform;
import org.talend.components.jdbc.output.platforms.MySQLPlatform;
import org.talend.components.jdbc.output.platforms.OraclePlatform;
import org.talend.components.jdbc.output.platforms.Platform;
import org.talend.components.jdbc.output.platforms.PostgreSQLPlatform;
import org.talend.components.jdbc.output.platforms.RedshiftPlatform;
import org.talend.components.jdbc.output.platforms.SQLDWHPlatform;
import org.talend.components.jdbc.output.platforms.SnowflakePlatform;
import org.talend.components.jdbc.service.I18nMessage;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.api.service.configuration.Configuration;

import java.util.Locale;
import java.util.function.Supplier;

import static java.util.Optional.ofNullable;
import static org.talend.components.jdbc.output.platforms.DerbyPlatform.DERBY;
import static org.talend.components.jdbc.output.platforms.MSSQLPlatform.MSSQL;
import static org.talend.components.jdbc.output.platforms.MSSQLPlatform.MSSQL_JTDS;
import static org.talend.components.jdbc.output.platforms.MariaDbPlatform.MARIADB;
import static org.talend.components.jdbc.output.platforms.MySQLPlatform.MYSQL;
import static org.talend.components.jdbc.output.platforms.OraclePlatform.ORACLE;
import static org.talend.components.jdbc.output.platforms.PostgreSQLPlatform.POSTGRESQL;
import static org.talend.components.jdbc.output.platforms.RedshiftPlatform.REDSHIFT;
import static org.talend.components.jdbc.output.platforms.SQLDWHPlatform.SQLDWH;
import static org.talend.components.jdbc.output.platforms.SnowflakePlatform.SNOWFLAKE;

@Service
public class PlatformFactory {

    // public static Platform get(final String dbType, final I18nMessage i18n) {
    public Platform get(final JdbcConfiguration.Driver driver, final I18nMessage i18n) {
        // final String dbType = ofNullable(connection.getHandler()).orElseGet(connection::getDbType);

        final String dbType = driver.getId();
        switch (dbType.toLowerCase(Locale.ROOT)) {
        case MYSQL:
            return new MySQLPlatform(i18n, driver);
        case MARIADB:
            return new MariaDbPlatform(i18n, driver);
        case POSTGRESQL:
            return new PostgreSQLPlatform(i18n, driver);
        case REDSHIFT:
            return new RedshiftPlatform(i18n, driver);
        case SNOWFLAKE:
            return new SnowflakePlatform(i18n, driver);
        case ORACLE:
            return new OraclePlatform(i18n, driver);
        case MSSQL:
        case MSSQL_JTDS:
            return new MSSQLPlatform(i18n, driver);
        case SQLDWH:
            return new SQLDWHPlatform(i18n, driver);
        case DERBY:
        default:
            return new DerbyPlatform(i18n, driver);
        }

    }
}
