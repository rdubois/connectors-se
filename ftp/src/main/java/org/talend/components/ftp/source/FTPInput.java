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
package org.talend.components.ftp.source;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPFile;
import org.talend.components.common.stream.api.RecordIORepository;
import org.talend.components.common.stream.api.input.RecordReader;
import org.talend.components.common.stream.format.ContentFormat;
import org.talend.components.ftp.service.FTPService;
import org.talend.components.ftp.service.I18nMessage;
import org.talend.components.ftp.service.ftpclient.GenericFTPClient;
import org.talend.components.ftp.service.ftpclient.GenericFTPFile;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.input.Producer;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;

import javax.annotation.PreDestroy;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

@Slf4j
@RequiredArgsConstructor
public class FTPInput implements Serializable {

    protected final FTPInputConfiguration configuration;

    protected final FTPService ftpService;

    protected final RecordBuilderFactory recordBuilderFactory;

    protected final I18nMessage i18n;

    protected final RecordIORepository recordIORepository;

    protected List<GenericFTPFile> filesToRead;

    private transient Iterator<GenericFTPFile> fileIterator;

    private transient Iterator<Record> recordIterator;

    private transient GenericFTPClient ftpClient;

    private transient RecordReader recordReader;

    private transient boolean init = false;

    protected FTPInput(FTPInputConfiguration configuration, FTPService ftpService, RecordBuilderFactory recordBuilderFactory,
            I18nMessage i18n, RecordIORepository recordIORepository, List<GenericFTPFile> filesToRead) {
        this.configuration = configuration;
        this.ftpService = ftpService;
        this.recordBuilderFactory = recordBuilderFactory;
        this.i18n = i18n;
        this.recordIORepository = recordIORepository;
        if (filesToRead != null) {
            this.filesToRead = new ArrayList<>();
            this.filesToRead.addAll(filesToRead);
        }
    }

    @Producer
    public Object next() {
        if (!init) {
            init = true;
            if (filesToRead == null) {
                GenericFTPClient currentClient = getFtpClient();
                try {

                    filesToRead = currentClient.listFiles(configuration.getDataSet().getPath(), f -> !(f.isDirectory()));
                } catch (Exception e) {
                    e.printStackTrace();
                }

            }
            fileIterator = filesToRead.iterator();
            ContentFormat contentFormat = configuration.getDataSet().getFormatConfiguration();
            recordReader = recordIORepository.findReader(contentFormat.getClass()).getReader(recordBuilderFactory, contentFormat);
        }

        if (recordIterator == null || !recordIterator.hasNext()) {
            // No more record to read in current file, or first fille to be read
            if (fileIterator.hasNext()) {
                GenericFTPFile file = fileIterator.next();
                final ByteArrayOutputStream buffer = new ByteArrayOutputStream();

                String path = configuration.getDataSet().getPath()
                        + (configuration.getDataSet().getPath().endsWith(FTPService.PATH_SEPARATOR) ? ""
                                : FTPService.PATH_SEPARATOR)
                        + file.getName();
                getFtpClient().retrieveFile(path, buffer);
                recordIterator = recordReader.read(new ByteArrayInputStream(buffer.toByteArray()));
            }
        }

        if (recordIterator != null && recordIterator.hasNext()) {
            return recordIterator.next();
        }

        return null;
    }

    private GenericFTPClient getFtpClient() {
        if (ftpClient == null || !ftpClient.isConnected()) {
            log.debug("Creating new client");
            ftpClient = ftpService.getClient(configuration.getDataSet().getDatastore());
            if (configuration.isDebug()) {
                ftpClient.enableDebug(log);
            }

            ftpClient.configure(configuration);
        }

        return ftpClient;
    }

    @PreDestroy
    public void release() {
        if (ftpClient != null) {
            ftpClient.disconnect();
        }
        ftpClient = null;
    }

}
