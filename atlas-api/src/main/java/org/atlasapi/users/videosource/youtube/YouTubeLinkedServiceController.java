package org.atlasapi.users.videosource.youtube;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.atlasapi.application.auth.UserFetcher;
import org.atlasapi.application.model.auth.OAuthRequest;
import org.atlasapi.application.sources.SourceIdCodec;
import org.atlasapi.application.users.User;
import org.atlasapi.entity.Id;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.output.ErrorResultWriter;
import org.atlasapi.output.ErrorSummary;
import org.atlasapi.output.NotAcceptableException;
import org.atlasapi.output.NotAuthorizedException;
import org.atlasapi.output.QueryResultWriter;
import org.atlasapi.output.ResponseWriter;
import org.atlasapi.output.ResponseWriterFactory;
import org.atlasapi.output.UnsupportedFormatException;
import org.atlasapi.query.common.QueryResult;
import org.atlasapi.query.common.context.QueryContext;
import org.atlasapi.users.videosource.UserVideoSourceStore;
import org.atlasapi.users.videosource.model.OauthToken;
import org.atlasapi.users.videosource.model.UserVideoSource;
import org.atlasapi.users.videosource.model.VideoSourceChannel;
import org.atlasapi.users.videosource.model.VideoSourceChannelResults;
import org.atlasapi.users.videosource.remote.RemoteSourceUpdaterClient;

import com.metabroadcast.common.ids.NumberToShortStringCodec;
import com.metabroadcast.common.social.model.UserRef;
import com.metabroadcast.common.social.model.UserRef.UserNamespace;
import com.metabroadcast.common.url.QueryStringParameters;
import com.metabroadcast.common.url.Urls;

import com.google.api.client.auth.oauth2.AuthorizationCodeResponseUrl;
import com.google.api.client.auth.oauth2.Credential;
import com.google.api.client.googleapis.auth.oauth2.GoogleAuthorizationCodeRequestUrl;
import com.google.api.client.googleapis.auth.oauth2.GoogleAuthorizationCodeTokenRequest;
import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.auth.oauth2.GoogleTokenResponse;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.client.repackaged.com.google.common.base.Joiner;
import com.google.api.client.repackaged.com.google.common.base.Preconditions;
import com.google.api.services.oauth2.Oauth2;
import com.google.api.services.oauth2.model.Userinfo;
import com.google.api.services.youtube.YouTube;
import com.google.api.services.youtube.model.Channel;
import com.google.api.services.youtube.model.ChannelListResponse;
import com.google.common.base.Function;
import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;
import org.elasticsearch.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;

@Controller
public class YouTubeLinkedServiceController {

    private static Logger log = LoggerFactory.getLogger(YouTubeLinkedServiceController.class);
    private static final String YT_APP_NAME = "Atlas";
    private static final String STATE_BOUNDARY = "==";
    private static final Joiner STATE_JOINER = Joiner.on(STATE_BOUNDARY);
    private static final Splitter STATE_SPLITTER = Splitter.on(STATE_BOUNDARY);
    private static final List<String> SCOPES_REQUIRED = Arrays.asList(
            "https://www.googleapis.com/auth/youtube.readonly",
            "https://www.googleapis.com/auth/userinfo.profile"
    );
    private final ResponseWriterFactory writerResolver = new ResponseWriterFactory();
    private final String youTubeClientId;
    private final String youTubeClientSecret;
    private final QueryResultWriter<OAuthRequest> oauthRequestResultWriter;
    private final JsonFactory jsonFactory = new JacksonFactory();
    private final HttpTransport httpTransport = new NetHttpTransport();
    private final UserFetcher userFetcher;
    private final NumberToShortStringCodec idCodec;
    private final SourceIdCodec sourceIdCodec;
    private final UserVideoSourceStore store;
    private final RemoteSourceUpdaterClient sourceUpdaterClient;
    private final QueryResultWriter<VideoSourceChannelResults> resultWriter;

    private static final Function<Channel, VideoSourceChannel> YT_CHANNELS_TRANSLATOR = new Function<Channel, VideoSourceChannel>() {

        @Override
        public VideoSourceChannel apply(Channel input) {
            return VideoSourceChannel.builder()
                    .withId(input.getId())
                    .withTitle(input.getSnippet().getTitle())
                    .withImageUrl(input.getSnippet().getThumbnails().getDefault().getUrl())
                    .build();
        }
    };

    public YouTubeLinkedServiceController(String youTubeClientId,
            String youTubeClientSecret,
            QueryResultWriter<OAuthRequest> oauthRequestResultWriter,
            UserFetcher userFetcher,
            NumberToShortStringCodec idCodec,
            SourceIdCodec sourceIdCodec,
            UserVideoSourceStore store,
            RemoteSourceUpdaterClient sourceUpdaterClient,
            QueryResultWriter<VideoSourceChannelResults> resultWriter) {
        super();
        Preconditions.checkNotNull(youTubeClientId);
        Preconditions.checkNotNull(youTubeClientSecret);
        Preconditions.checkNotNull(oauthRequestResultWriter);
        Preconditions.checkNotNull(userFetcher);
        Preconditions.checkNotNull(idCodec);
        Preconditions.checkNotNull(sourceIdCodec);
        Preconditions.checkNotNull(store);
        Preconditions.checkNotNull(sourceUpdaterClient);
        Preconditions.checkNotNull(resultWriter);

        this.youTubeClientId = youTubeClientId;
        this.youTubeClientSecret = youTubeClientSecret;
        this.oauthRequestResultWriter = oauthRequestResultWriter;
        this.userFetcher = userFetcher;
        this.idCodec = idCodec;
        this.sourceIdCodec = sourceIdCodec;
        this.store = store;
        this.sourceUpdaterClient = sourceUpdaterClient;
        this.resultWriter = resultWriter;
    }

    // Any callback URL used must be registered in the Google API console
    @RequestMapping(value = { "/4/videosource/youtube/login.*" }, method = RequestMethod.GET)
    public void getYouTubeLinkLogin(HttpServletRequest request,
            HttpServletResponse response,
            @RequestParam(required = true) String redirectUri) throws UnsupportedFormatException,
            NotAcceptableException, IOException {
        User user = userFetcher.userFor(request).get();
        String userId = idCodec.encode(user.getId().toBigInteger());
        ResponseWriter writer = writerResolver.writerFor(request, response);
        String atlasCallbackUri = request.getRequestURL().toString().replace("login", "token");
        // Put original callback uri into state so that we can redirect to it
        String state = STATE_JOINER.join(userId, redirectUri);
        String authUrl = new GoogleAuthorizationCodeRequestUrl(youTubeClientId,
                atlasCallbackUri, SCOPES_REQUIRED
        )
                .setAccessType("offline")
                .setState(state)
                .setApprovalPrompt("force")
                .build();
        OAuthRequest oauthRequest = OAuthRequest.builder()
                .withUuid(OAuthRequest.generateUuid())
                .withNamespace(UserNamespace.YOUTUBE)
                .withAuthUrl(authUrl)
                .withCallbackUrl(redirectUri)
                .withToken("")
                .withSecret("")
                .build();
        QueryResult<OAuthRequest> queryResult = QueryResult.singleResult(
                oauthRequest,
                QueryContext.standard(request)
        );
        oauthRequestResultWriter.write(queryResult, writer);
    }

    @RequestMapping(value = { "/4/videosource/youtube/token.*" }, method = RequestMethod.GET)
    public void getAccessToken(HttpServletResponse response, HttpServletRequest request)
            throws IOException {
        StringBuffer fullUrlBuf = request.getRequestURL();
        if (request.getQueryString() != null) {
            fullUrlBuf.append('?').append(request.getQueryString());
        }
        // decode the information in the state
        List<String> stateParts = STATE_SPLITTER.splitToList(request.getParameter("state"));
        String userId = stateParts.get(0);
        String callbackUri = stateParts.get(1);

        try {
            AuthorizationCodeResponseUrl authResponse = new AuthorizationCodeResponseUrl(
                    fullUrlBuf.toString());
            if (authResponse.getError() != null) {
                String errorMsg = String.format(
                        "Error received from remote Oauth server: %s",
                        authResponse.getError()
                );
                throw new NotAcceptableException(errorMsg);
            }
            GoogleTokenResponse googleTokenResponse = getGoogleTokenResponse(
                    authResponse,
                    request.getRequestURL().toString()
            );
            OauthToken token = getOauthTokenFromTokenResponse(googleTokenResponse);
            // get the youtube user info
            Userinfo userinfo = getUserInfo(googleTokenResponse);
            // get the passed atlas user from the scope
            Id atlasUser = Id.valueOf(idCodec.decode(userId));
            // Future ticket: user assigned publisher
            UserVideoSource userVideoSource = UserVideoSource.builder()
                    .withUserRef(new UserRef(userinfo.getId(), UserNamespace.YOUTUBE))
                    .withAtlasUser(atlasUser)
                    .withName(userinfo.getName())
                    .withPublisher(Publisher.YOUTUBE)
                    .withToken(token)
                    .build();
            store.store(userVideoSource);
            response.sendRedirect(callbackUri);
        } catch (Exception e) {
            // Redirect back to original callback uri with error
            log.error("Request exception " + request.getRequestURI(), e);
            ErrorSummary summary = ErrorSummary.forException(e);
            QueryStringParameters errorParams = QueryStringParameters
                    .parameters("error_code", summary.errorCode())
                    .add("error", summary.message());
            response.sendRedirect(Urls.appendParameters(callbackUri, errorParams));
        }
    }

    @RequestMapping(value = { "/4/videosource/youtube/channels.*" }, method = RequestMethod.GET)
    public void getChannels(HttpServletResponse response, HttpServletRequest request)
            throws IOException {
        ResponseWriter writer = null;
        try {
            User user = userFetcher.userFor(request).get();
            writer = writerResolver.writerFor(request, response);
            List<VideoSourceChannelResults> channels = getChannelsForUser(user);
            QueryResult<VideoSourceChannelResults> queryResult =
                    QueryResult.listResult(
                            channels,
                            QueryContext.standard(request),
                            channels.size()
                    );
            resultWriter.write(queryResult, writer);
        } catch (Exception e) {
            log.error("Request exception " + request.getRequestURI(), e);
            ErrorSummary summary = ErrorSummary.forException(e);
            new ErrorResultWriter().write(summary, writer, request, response);
        }
    }

    @RequestMapping(value = { "/4/videosource/youtube/{youtubeId}/source/add/{sourceId}.*" },
            method = RequestMethod.POST)
    public void addSource(HttpServletResponse response, HttpServletRequest request,
            @PathVariable String youtubeId,
            @PathVariable String sourceId) throws IOException {
        // Once we have the source we can post the user details to the remote service
        ResponseWriter writer = null;
        try {
            Publisher source = sourceIdCodec.decode(sourceId).get();
            User user = userFetcher.userFor(request).get();
            UserVideoSource userVideoSource = store.sourceForRemoteuserRef(new UserRef(
                    youtubeId,
                    UserNamespace.YOUTUBE
            ));
            if (userVideoSource.getAtlasUser().equals(user.getId())) {
                UserVideoSource sourceWithPublisher = userVideoSource.copy()
                        .withPublisher(source)
                        .build();
                store.store(sourceWithPublisher);
                sourceUpdaterClient.register(sourceWithPublisher);
                sourceUpdaterClient.addToken(sourceWithPublisher, userVideoSource.getToken());
            } else {
                throw new NotAuthorizedException();
            }
        } catch (Exception e) {
            log.error("Request exception " + request.getRequestURI(), e);
            ErrorSummary summary = ErrorSummary.forException(e);
            new ErrorResultWriter().write(summary, writer, request, response);
        }
    }

    @RequestMapping(value = { "/4/videosource/youtube/{youtubeId}/channels/add/{channelId}.*" },
            method = RequestMethod.POST)
    public void addChannel(HttpServletResponse response, HttpServletRequest request,
            @PathVariable String youtubeId,
            @PathVariable String channelId) throws IOException {
        ResponseWriter writer = null;
        try {
            User user = userFetcher.userFor(request).get();
            UserVideoSource userVideoSource = store.sourceForRemoteuserRef(new UserRef(
                    youtubeId,
                    UserNamespace.YOUTUBE
            ));
            if (userVideoSource.getAtlasUser().equals(user.getId())) {
                sourceUpdaterClient.addChannelId(userVideoSource, channelId);
                store.store(userVideoSource.copy().addChannelId(channelId).build());
            } else {
                throw new NotAuthorizedException();
            }
        } catch (Exception e) {
            log.error("Request exception " + request.getRequestURI(), e);
            ErrorSummary summary = ErrorSummary.forException(e);
            new ErrorResultWriter().write(summary, writer, request, response);
        }
    }

    private Credential getCredentialForToken(GoogleTokenResponse tokenResponse) {
        return new GoogleCredential.Builder()
                .setJsonFactory(jsonFactory)
                .setTransport(httpTransport)
                .setClientSecrets(youTubeClientId, youTubeClientSecret).build()
                .setFromTokenResponse(tokenResponse);
    }

    private GoogleTokenResponse getGoogleTokenResponse(AuthorizationCodeResponseUrl authResponse,
            String redirectUri) throws IOException {
        return new GoogleAuthorizationCodeTokenRequest(
                httpTransport,
                jsonFactory,
                youTubeClientId,
                youTubeClientSecret,
                authResponse.getCode(),
                redirectUri
        )
                .execute();
    }

    private OauthToken getOauthTokenFromTokenResponse(GoogleTokenResponse tokenResponse) {
        return OauthToken.builder()
                .withAccessToken(tokenResponse.getAccessToken())
                .withTokenType(tokenResponse.getTokenType())
                .withExpiresIn(tokenResponse.getExpiresInSeconds())
                .withIdToken(tokenResponse.getIdToken())
                .withRefreshToken(tokenResponse.getRefreshToken())
                .build();
    }

    private Userinfo getUserInfo(GoogleTokenResponse tokenResponse) throws IOException {
        Credential credential = getCredentialForToken(tokenResponse);
        Oauth2 oauth2 = new Oauth2.Builder(
                httpTransport,
                jsonFactory,
                credential
        ).setApplicationName(
                YT_APP_NAME)
                .build();
        return oauth2.userinfo().get().execute();
    }

    private List<VideoSourceChannelResults> getChannelsForUser(User user) throws IOException {
        Iterable<UserVideoSource> userVideoSources = store.userVideoSourcesFor(
                user,
                UserNamespace.YOUTUBE
        );
        List<VideoSourceChannelResults> channelResults = Lists.newArrayList();
        for (UserVideoSource userVideoSource : userVideoSources) {
            YouTube youtube = new YouTube.Builder(httpTransport,
                    jsonFactory, null
            ).setApplicationName(YT_APP_NAME).build();
            YouTube.Channels.List channelRequest = youtube.channels().list("id,snippet");
            channelRequest.setOauthToken(userVideoSource.getToken().getAccessToken());
            channelRequest.setMine(true);
            ChannelListResponse channelResponse = channelRequest.execute();
            Iterable<VideoSourceChannel> channels = Iterables.transform(
                    channelResponse.getItems(),
                    YT_CHANNELS_TRANSLATOR
            );
            VideoSourceChannelResults results = VideoSourceChannelResults.builder()
                    .withId(userVideoSource.getUserRef().getUserId())
                    .withChannels(channels)
                    .build();
            channelResults.add(results);
        }
        return channelResults;
    }
}
