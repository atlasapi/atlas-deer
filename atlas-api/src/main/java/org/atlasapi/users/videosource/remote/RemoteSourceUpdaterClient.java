package org.atlasapi.users.videosource.remote;

import javax.servlet.http.HttpServletResponse;

import org.atlasapi.output.NotAcceptableException;
import org.atlasapi.users.videosource.model.OauthToken;
import org.atlasapi.users.videosource.model.UserVideoSource;

import com.metabroadcast.common.http.HttpException;
import com.metabroadcast.common.http.HttpResponse;
import com.metabroadcast.common.http.Payload;
import com.metabroadcast.common.http.SimpleHttpClient;
import com.metabroadcast.common.http.StringPayload;

import com.google.gson.Gson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RemoteSourceUpdaterClient {

    private final Logger log = LoggerFactory.getLogger(RemoteSourceUpdaterClient.class);
    private static final String ADD_USER_URL = "/user/add";
    private static final String ADD_TOKEN_URL = "/user/%s/token";
    private static final String ADD_CHANNEL_URL = "/user/%s/channels/add/%s";
    private static final String REMOVE_CHANNEL_URL = "/user/%s/channels/remove/%s";
    private final Gson gson;
    private final SimpleHttpClient httpClient;
    private final String server;

    public RemoteSourceUpdaterClient(Gson gson, String server, SimpleHttpClient httpClient) {
        this.gson = gson;
        this.httpClient = httpClient;
        this.server = server;
    }

    public void register(UserVideoSource userVideoSource)
            throws HttpException, NotAcceptableException {
        postToHandlingService(ADD_USER_URL, new StringPayload(gson.toJson(userVideoSource)));
    }

    public void addToken(UserVideoSource userVideoSource, OauthToken token)
            throws HttpException, NotAcceptableException {
        String url = String.format(ADD_TOKEN_URL, userVideoSource.getUserRef().getUserId());
        postToHandlingService(url, new StringPayload(gson.toJson(token)));
    }

    public void addChannelId(UserVideoSource userVideoSource, String channelId)
            throws HttpException, NotAcceptableException {
        String url = String.format(
                ADD_CHANNEL_URL,
                userVideoSource.getUserRef().getUserId(),
                channelId
        );
        postToHandlingService(url, new StringPayload(""));
    }

    public void removeChannelIdFromUser(UserVideoSource userVideoSource, String channelId)
            throws HttpException, NotAcceptableException {
        String url = String.format(
                REMOVE_CHANNEL_URL,
                userVideoSource.getUserRef().getUserId(),
                channelId
        );
        postToHandlingService(url, new StringPayload(""));
    }

    private void postToHandlingService(String url, Payload payload)
            throws HttpException, NotAcceptableException {
        HttpResponse response = httpClient.post(server + url, payload);
        if (response.statusCode() == HttpServletResponse.SC_OK) {
            log.trace(String.format(
                    "%s %s for url %s",
                    response.statusCode(),
                    response.statusLine(),
                    url
            ));
        } else {
            // Log the error but do not reveal details about remote handling host
            log.error(String.format(
                    "%s %s when url %s",
                    response.statusCode(),
                    response.statusLine(),
                    url
            ));
            throw new NotAcceptableException("Handling service could not process request.");
        }
    }
}
