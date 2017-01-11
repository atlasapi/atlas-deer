package org.atlasapi.application.auth.google;

import java.io.IOException;
import java.util.Set;
import java.util.UUID;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.atlasapi.application.model.auth.OAuthProvider;
import org.atlasapi.application.model.auth.OAuthRequest;
import org.atlasapi.application.model.auth.OAuthResult;
import org.atlasapi.application.model.auth.TokenRequestStore;
import org.atlasapi.application.users.NewUserSupplier;
import org.atlasapi.application.users.User;
import org.atlasapi.application.users.UserStore;
import org.atlasapi.output.ErrorResultWriter;
import org.atlasapi.output.ErrorSummary;
import org.atlasapi.output.NotAcceptableException;
import org.atlasapi.output.QueryResultWriter;
import org.atlasapi.output.ResponseWriter;
import org.atlasapi.output.ResponseWriterFactory;
import org.atlasapi.output.UnsupportedFormatException;
import org.atlasapi.query.common.QueryResult;
import org.atlasapi.query.common.context.QueryContext;

import com.metabroadcast.common.base.Maybe;
import com.metabroadcast.common.social.auth.credentials.AuthToken;
import com.metabroadcast.common.social.model.UserRef;
import com.metabroadcast.common.social.model.UserRef.UserNamespace;
import com.metabroadcast.common.social.user.AccessTokenProcessor;
import com.metabroadcast.common.url.UrlEncoding;
import com.metabroadcast.common.url.Urls;
import com.metabroadcast.common.webapp.http.CacheHeaderWriter;

import com.google.api.services.oauth2.model.Userinfo;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;

import static com.google.api.client.repackaged.com.google.common.base.Preconditions.checkNotNull;

@Controller
public class GoogleAuthController {

    private static final Set<String> SCOPES_REQUIRED = ImmutableSet.of(
            "profile",
            "https://www.googleapis.com/auth/userinfo.email",
            "https://www.googleapis.com/auth/userinfo.profile"
    );

    private static final CacheHeaderWriter NO_CACHE_HEADER_WRITER = CacheHeaderWriter.neverCache();

    private static Logger log = LoggerFactory.getLogger(GoogleAuthController.class);
    private final ResponseWriterFactory writerResolver = new ResponseWriterFactory();
    private final GoogleAuthClient googleClient;
    private final UserStore userStore;
    private final NewUserSupplier userSupplier;
    private final QueryResultWriter<OAuthRequest> oauthRequestResultWriter;
    private final QueryResultWriter<OAuthResult> oauthResultResultWriter;
    private final AccessTokenProcessor accessTokenProcessor;
    private final TokenRequestStore tokenRequestStore;

    public GoogleAuthController(GoogleAuthClient googleClient,
            UserStore userStore,
            NewUserSupplier userSupplier,
            QueryResultWriter<OAuthRequest> oauthRequestResultWriter,
            QueryResultWriter<OAuthResult> oauthResultResultWriter,
            AccessTokenProcessor accessTokenProcessor, TokenRequestStore tokenRequestStore) {
        super();
        this.googleClient = checkNotNull(googleClient);
        this.userStore = checkNotNull(userStore);
        this.userSupplier = checkNotNull(userSupplier);
        this.oauthRequestResultWriter = checkNotNull(oauthRequestResultWriter);
        this.oauthResultResultWriter = checkNotNull(oauthResultResultWriter);
        this.accessTokenProcessor = checkNotNull(accessTokenProcessor);
        this.tokenRequestStore = checkNotNull(tokenRequestStore);
    }

    @RequestMapping(value = { "/4/auth/google/login.*" }, method = RequestMethod.GET)
    public void getGoogleLogin(HttpServletRequest request,
            HttpServletResponse response,
            @RequestParam(required = true) String callbackUrl,
            @RequestParam(required = false) String targetUri)
            throws UnsupportedFormatException, NotAcceptableException, IOException {
        NO_CACHE_HEADER_WRITER.writeHeaders(request, response);
        ResponseWriter writer = writerResolver.writerFor(request, response);
        if (!Strings.isNullOrEmpty(targetUri)) {
            callbackUrl += "?targetUri=" + UrlEncoding.encode(targetUri);
        }
        UUID requestUuid = OAuthRequest.generateUuid();
        String atlasCallbackUri = request.getRequestURL().toString().replace("login", "callback");
        String authUrl = googleClient.getAuthorizationCodeRequestUrl(
                SCOPES_REQUIRED,
                atlasCallbackUri,
                requestUuid.toString()
        );

        OAuthRequest oauthRequest = OAuthRequest.builder()
                .withUuid(requestUuid)
                .withNamespace(UserNamespace.GOOGLE)
                .withAuthUrl(authUrl)
                .withCallbackUrl(callbackUrl)
                .withToken("")
                .withSecret("")
                .build();
        tokenRequestStore.store(oauthRequest);
        QueryResult<OAuthRequest> queryResult = QueryResult.singleResult(
                oauthRequest,
                QueryContext.standard(request)
        );
        oauthRequestResultWriter.write(queryResult, writer);
    }

    @RequestMapping(value = { "/4/auth/google/callback.json" }, method = RequestMethod.GET)
    public void handleOauthCallback(HttpServletResponse response, HttpServletRequest request,
            @RequestParam(required = false) String code,
            @RequestParam(required = false) String state,
            @RequestParam(required = false) String error) throws IOException {
        NO_CACHE_HEADER_WRITER.writeHeaders(request, response);
        OAuthRequest oauthRequest = tokenRequestStore.lookupAndRemove(UUID.fromString(state)).get();
        String redirectUrl = oauthRequest.getCallbackUrl().toExternalForm();
        if (!Strings.isNullOrEmpty(code)) {
            redirectUrl = Urls.appendParameters(redirectUrl, "code", code);
        } else {
            redirectUrl = Urls.appendParameters(redirectUrl, "error", error);
        }
        response.sendRedirect(redirectUrl);
    }

    @RequestMapping(value = { "/4/auth/google/token.*" }, method = RequestMethod.GET)
    public void getAccessToken(HttpServletResponse response, HttpServletRequest request,
            @RequestParam String code,
            @RequestParam(required = false) String targetUri) throws IOException {
        ResponseWriter writer = null;
        try {
            writer = writerResolver.writerFor(request, response);
            String redirectUri = request.getRequestURL().toString().replace("token", "callback");
            AuthToken token = googleClient.getAuthTokenForCode(code, redirectUri);
            Maybe<UserRef> userRef = accessTokenProcessor.process(token);
            boolean success = userRef.hasValue();
            OAuthResult.Builder oauthResult = OAuthResult.builder()
                    .withProvider(OAuthProvider.GOOGLE)
                    .withSuccess(success)
                    .withToken("");

            if (success) {
                // Make sure we have a user 
                updateUser(token, userRef.requireValue());
                oauthResult.withToken(token.tokenPayload());
            }

            QueryResult<OAuthResult> queryResult = QueryResult.singleResult(
                    oauthResult.build(),
                    QueryContext.standard(request)
            );
            oauthResultResultWriter.write(queryResult, writer);
        } catch (Exception e) {
            log.error("Request exception " + request.getRequestURI(), e);
            ErrorSummary summary = ErrorSummary.forException(e);
            new ErrorResultWriter().write(summary, writer, request, response);
        }
    }

    private void updateUser(AuthToken token, UserRef userRef) throws IOException {
        Userinfo googleUser = googleClient.getUserForToken(token);
        User user = userStore.userForRef(userRef).or(userSupplier);
        if (user.getUserRef() == null) {
            user = user.copy().withUserRef(userRef).build();
        }
        if (user.isProfileComplete()) {
            return;
        }

        // If profile not complete then populate with info from Google
        User.Builder modified = user.copy()
                .withScreenName(googleUser.getName())
                .withFullName(googleUser.getName())
                .withProfileImage(googleUser.getPicture())
                .withEmail(googleUser.getEmail())
                .withWebsite(googleUser.getLink())
                .withProfileComplete(false);
        userStore.store(modified.build());
    }
}
