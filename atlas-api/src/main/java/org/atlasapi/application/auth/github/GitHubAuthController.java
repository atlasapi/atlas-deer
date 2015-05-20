package org.atlasapi.application.auth.github;

import static com.google.api.client.repackaged.com.google.common.base.Preconditions.checkNotNull;

import java.io.IOException;
import java.util.Set;

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
import org.atlasapi.query.common.QueryContext;
import org.atlasapi.query.common.QueryResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableSet;
import com.metabroadcast.common.base.Maybe;
import com.metabroadcast.common.social.auth.credentials.AuthToken;
import com.metabroadcast.common.social.model.UserRef;
import com.metabroadcast.common.social.model.UserRef.UserNamespace;
import com.metabroadcast.common.social.user.AccessTokenProcessor;
import com.metabroadcast.common.url.UrlEncoding;
import com.metabroadcast.common.webapp.http.CacheHeaderWriter;

@Controller
public class GitHubAuthController {
    private static Logger log = LoggerFactory.getLogger(GitHubAuthController.class);
    
    private static final CacheHeaderWriter NO_CACHE_HEADER_WRITER = CacheHeaderWriter.neverCache();
    
    private final ResponseWriterFactory writerResolver = new ResponseWriterFactory();
    private final GitHubAuthClient gitHubClient;
    private final UserStore userStore; 
    private final NewUserSupplier userSupplier;
    private final QueryResultWriter<OAuthRequest> oauthRequestResultWriter;
    private final QueryResultWriter<OAuthResult> oauthResultResultWriter;
    private final AccessTokenProcessor accessTokenProcessor;
    private final TokenRequestStore tokenRequestStore;
    
    private static final Set<String> SCOPES_REQUIRED = ImmutableSet.of(
            "user:email");
    
    public GitHubAuthController(
            GitHubAuthClient gitHubClient,
            AccessTokenProcessor accessTokenProcessor,
            UserStore userStore, 
            NewUserSupplier userSupplier,
            TokenRequestStore tokenRequestStore,
            QueryResultWriter<OAuthRequest> oauthRequestResultWriter,
            QueryResultWriter<OAuthResult> oauthResultResultWriter) {
        super();
        this.gitHubClient = checkNotNull(gitHubClient);
        this.userStore = checkNotNull(userStore);
        this.userSupplier = checkNotNull(userSupplier);
        this.oauthRequestResultWriter = checkNotNull(oauthRequestResultWriter);
        this.oauthResultResultWriter = checkNotNull(oauthResultResultWriter);
        this.accessTokenProcessor = checkNotNull(accessTokenProcessor);
        this.tokenRequestStore = checkNotNull(tokenRequestStore);
    }
    
    @RequestMapping(value = { "/4/auth/github/login.*" }, method = RequestMethod.GET)
    public void getGitHubLogin(HttpServletRequest request,
        HttpServletResponse response,
            @RequestParam(required = true) String callbackUrl,
            @RequestParam(required = false) String targetUri) throws UnsupportedFormatException, NotAcceptableException, IOException {
        NO_CACHE_HEADER_WRITER.writeHeaders(request, response);
        ResponseWriter writer = writerResolver.writerFor(request, response);
        if (!Strings.isNullOrEmpty(targetUri)) {
            callbackUrl += "?targetUri=" + UrlEncoding.encode(targetUri);
        }
        String authUrl = gitHubClient.getAuthorizationCodeRequestUrl(SCOPES_REQUIRED, callbackUrl);
            
        OAuthRequest oauthRequest = OAuthRequest.builder()
             .withUuid(OAuthRequest.generateUuid())
             .withNamespace(UserNamespace.GITHUB)
             .withAuthUrl(authUrl)
             .withCallbackUrl(callbackUrl)
             .withToken("") 
             .withSecret("") 
             .build();
        tokenRequestStore.store(oauthRequest);
        QueryResult<OAuthRequest> queryResult = QueryResult.singleResult(oauthRequest, QueryContext.standard(request));
        oauthRequestResultWriter.write(queryResult, writer);
    }

    @RequestMapping(value = { "/4/auth/github/token.*" }, method = RequestMethod.GET)
    public void getAccessToken(HttpServletResponse response, HttpServletRequest request, 
            @RequestParam String code,
            @RequestParam(required = false) String targetUri) throws IOException {
        ResponseWriter writer = null;
        try {
            writer = writerResolver.writerFor(request, response);
            AuthToken token = gitHubClient.getAuthTokenForCode(code);           
            Maybe<UserRef> userRef = accessTokenProcessor.process(token);
            OAuthResult oauthResult;
            if (userRef.hasValue()) {
                // Make sure we have a user 
                updateUser(token, userRef.requireValue());
                oauthResult = OAuthResult.builder()
                        .withSuccess(true)
                        .withProvider(OAuthProvider.GITHUB)
                        .withToken(token.tokenPayload())
                        .build();
            } else {
                oauthResult = OAuthResult.builder()
                        .withSuccess(false)
                        .withProvider(OAuthProvider.GITHUB)
                        .withToken("")
                        .build();
            }
            
            QueryResult<OAuthResult> queryResult = QueryResult.singleResult(oauthResult, QueryContext.standard(request));
            oauthResultResultWriter.write(queryResult, writer);
        }  catch (Exception e) {
            log.error("Request exception " + request.getRequestURI(), e);
            ErrorSummary summary = ErrorSummary.forException(e);
            new ErrorResultWriter().write(summary, writer, request, response);
        }
    }
    
    private void updateUser(AuthToken token, UserRef userRef) throws IOException {
        GitHubUser gitHubUser = gitHubClient.getUserForToken(token);
        User user = userStore.userForRef(userRef).or(userSupplier);
        if (user.getUserRef() == null) {
            user = user.copy().withUserRef(userRef).build();
        }
        if (user.isProfileComplete()) {
            return;
        }
        // If profile not complete then populate with info from GitHub
        String website = "";
        if (!Strings.isNullOrEmpty(gitHubUser.getBlog())) {
            website = gitHubUser.getBlog();
        } 
        else if (!Strings.isNullOrEmpty(gitHubUser.getHtmlUrl())) {
            website = gitHubUser.getHtmlUrl();
        }

        User.Builder modified = user.copy()
            .withScreenName(gitHubUser.getLogin())
            .withFullName(gitHubUser.getName())
            .withProfileImage(gitHubUser.getAvatarUrl())
            .withCompany(gitHubUser.getCompany())
            .withWebsite(website)
            .withProfileComplete(false);

        try {
            modified = modified.withEmail(gitHubClient.getPrimaryEmailAddress(token));
        } catch (IllegalStateException | IOException e) {
           log.error("Could not get email from GitHub", e);
        }
        userStore.store(modified.build());
    }
}
