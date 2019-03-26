/*
 * Copyright (C) 2006-2018 Talend Inc. - www.talend.com
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

package org.talend.components.azure.common.csv;

import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.condition.ActiveIf;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.meta.Documentation;

@GridLayout(value = { @GridLayout.Row("recordDelimiter"), @GridLayout.Row("customRecordDelimiter"),
        @GridLayout.Row("fieldDelimiter"), @GridLayout.Row("customFieldDelimiter"), @GridLayout.Row("encoding"),
        @GridLayout.Row("customEncoding"), @GridLayout.Row("useHeader"), @GridLayout.Row("header") })
public class CSVFormatOptions {

    @Option
    @Documentation("Record delimiter")
    private RecordDelimiter recordDelimiter = RecordDelimiter.CRLF;

    @Option
    @ActiveIf(target = "recordDelimiter", value = "OTHER")
    @Documentation("Your custom record delimiter")
    private String customRecordDelimiter;

    @Option
    @Documentation("Field delimiter")
    private FieldDelimiter fieldDelimiter = FieldDelimiter.SEMICOLON;

    @Option
    @ActiveIf(target = "fieldDelimiter", value = "OTHER")
    @Documentation("Your custom field delimiter")
    private String customFieldDelimiter;

    @Option
    @Documentation("Encoding")
    private Encoding encoding = Encoding.UFT8;

    @Option
    @ActiveIf(target = "encoding", value = "OTHER")
    @Documentation("Your custom file encoding format")
    private String customEncoding;

    @Option
    @Documentation("Set header size")
    private boolean useHeader;

    @Option
    @ActiveIf(target = "useHeader", value = "true")
    @Documentation("Header size")
    private int header;
}