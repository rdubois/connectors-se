/*
 * Copyright (C) 2006-2019 Talend Inc. - www.talend.com
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
package org.talend.components.slack.service;

import org.omg.PortableServer.POA;
import org.talend.sdk.component.api.service.http.*;

import javax.json.JsonObject;

import static org.talend.components.slack.SlackApiConstants.*;

public interface MessagesClient extends HttpClient {

    /**
     * Returns a list of messages of a channel.
     *
     * @param accessToken Slack authorization bearer for API
     * @param contentType content type, e.g., application/x-www-form-urlencoded
     * @param channelName Slack Channel name
     */
    @Request(path = "/api/conversations.history", method = METHOD_POST)
    Response<JsonObject> getMessages( //
            @Header(HEADER_CONTENT_TYPE) String contentType, //
            @Header(ATTR_ACCESS_TOKEN) String accessToken, //
            @Query(ATTR_CHANNEL_ID) String channelName, //
            @Query("limit") int limit, //
            String body);

    @Request(path = "/api/conversations.list", method = METHOD_POST)
    Response<JsonObject> listChannels( //
            @Header(HEADER_CONTENT_TYPE) String contentType, //
            @Header(ATTR_ACCESS_TOKEN) String accessToken, //
            @Query("types") String type, //
            @Query("limit") int limit, //
            String body);

    @Request(path = "/api/auth.test", method = METHOD_POST)
    Response<JsonObject> checkAuth( //
            @Header(HEADER_CONTENT_TYPE) String contentType, //
            @Header(ATTR_ACCESS_TOKEN) String accessToken);

    @Request(path = "/api/users.list", method = METHOD_POST)
    Response<JsonObject> getUsers( //
            @Header(HEADER_CONTENT_TYPE) String contentType, //
            @Header(ATTR_ACCESS_TOKEN) String accessToken, //
            String body);

}