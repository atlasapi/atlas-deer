package org.atlasapi.application.auth.twitter;

import java.io.IOException;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.atlasapi.application.model.auth.OAuthProvider;
import org.atlasapi.application.model.auth.OAuthRequest;
import org.atlasapi.application.model.auth.OAuthResult;
import org.atlasapi.application.model.auth.TokenRequestStore;
import org.atlasapi.application.users.NewUserSupplier;
import org.atlasapi.application.users.User;
import org.atlasapi.application.users.UserStore;
import org.atlasapi.output.NotAcceptableException;
import org.atlasapi.output.QueryResultWriter;
import org.atlasapi.output.ResponseWriter;
import org.atlasapi.output.ResponseWriterFactory;
import org.atlasapi.output.UnsupportedFormatException;
import org.atlasapi.query.common.QueryContext;
import org.atlasapi.query.common.QueryResult;

import com.metabroadcast.common.base.Maybe;
import com.metabroadcast.common.social.auth.credentials.AuthToken;
import com.metabroadcast.common.social.model.TwitterUserDetails;
import com.metabroadcast.common.social.model.UserRef;
import com.metabroadcast.common.social.model.UserRef.UserNamespace;
import com.metabroadcast.common.social.twitter.TwitterApplication;
import com.metabroadcast.common.social.user.AccessTokenProcessor;
import com.metabroadcast.common.url.UrlEncoding;
import com.metabroadcast.common.webapp.http.CacheHeaderWriter;

import com.google.common.base.Optional;
import com.google.common.base.Strings;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import twitter4j.ResponseList;
import twitter4j.Twitter;
import twitter4j.TwitterException;
import twitter4j.TwitterFactory;
import twitter4j.auth.AccessToken;
import twitter4j.auth.RequestToken;
import twitter4j.conf.ConfigurationBuilder;

@Controller
public class TwitterAuthController {

    private static final CacheHeaderWriter NO_CACHE_HEADER_WRITER = CacheHeaderWriter.neverCache();

    private final ResponseWriterFactory writerResolver = new ResponseWriterFactory();
    private final TwitterFactory twitterFactory;
    private UserStore userStore;
    private NewUserSupplier userSupplier;
    private QueryResultWriter<OAuthRequest> oauthRequestResultWriter;
    private QueryResultWriter<OAuthResult> oauthResultResultWriter;
    private AccessTokenProcessor accessTokenProcessor;
    private TokenRequestStore tokenRequestStore;

    public TwitterAuthController(TwitterApplication twitterApplication,
            AccessTokenProcessor accessTokenProcessor,
            UserStore userStore,
            NewUserSupplier userSupplier,
            TokenRequestStore tokenRequestStore,
            QueryResultWriter<OAuthRequest> oauthRequestResultWriter,
            QueryResultWriter<OAuthResult> oauthResultResultWriter) {
        super();
        this.accessTokenProcessor = accessTokenProcessor;
        this.twitterFactory = new TwitterFactory(
                new ConfigurationBuilder()
                        .setOAuthConsumerKey(twitterApplication.getConsumerKey())
                        .setOAuthConsumerSecret(twitterApplication.getConsumerSecret())
                        .build()
        );
        this.userStore = userStore;
        this.userSupplier = userSupplier;
        this.tokenRequestStore = tokenRequestStore;
        this.oauthRequestResultWriter = oauthRequestResultWriter;
        this.oauthResultResultWriter = oauthResultResultWriter;
    }

    @RequestMapping(value = { "/4/auth/twitter/login.*" }, method = RequestMethod.GET)
    public void getTwitterLogin(HttpServletRequest request,
            HttpServletResponse response,
            @RequestParam(required = true) String callbackUrl,
            @RequestParam(required = false) String targetUri)
            throws UnsupportedFormatException, NotAcceptableException, IOException {
        NO_CACHE_HEADER_WRITER.writeHeaders(request, response);
        ResponseWriter writer = writerResolver.writerFor(request, response);
        try {
            Twitter twitter = twitterFactory.getInstance();
            if (!Strings.isNullOrEmpty(targetUri)) {
                callbackUrl += "?targetUri=" + UrlEncoding.encode(targetUri);
            }
            RequestToken requestToken = twitter.getOAuthRequestToken(callbackUrl);
            OAuthRequest oauthRequest = OAuthRequest.builder()
                    .withUuid(OAuthRequest.generateUuid())
                    .withNamespace(UserNamespace.TWITTER)
                    .withAuthUrl(requestToken.getAuthenticationURL())
                    .withCallbackUrl(callbackUrl)
                    .withToken(requestToken.getToken())
                    .withSecret(requestToken.getTokenSecret())
                    .build();
            tokenRequestStore.store(oauthRequest);
            QueryResult<OAuthRequest> queryResult = QueryResult.singleResult(
                    oauthRequest,
                    QueryContext.standard(request)
            );
            oauthRequestResultWriter.write(queryResult, writer);
        } catch (TwitterException e) {
            throw new RuntimeException(e);
        }
    }

    @RequestMapping(value = { "/4/auth/twitter/token.*" }, method = RequestMethod.GET)
    public void getAccessToken(HttpServletResponse response, HttpServletRequest request,
            @RequestParam String oauthToken,
            @RequestParam String oauthVerifier,
            @RequestParam(required = false) String targetUri)
            throws UnsupportedFormatException, NotAcceptableException, IOException {
        NO_CACHE_HEADER_WRITER.writeHeaders(request, response);
        Twitter twitter = twitterFactory.getInstance();
        Optional<OAuthRequest> storedOAuthRequest = tokenRequestStore.lookupAndRemove(
                UserNamespace.TWITTER,
                oauthToken
        );
        if (!storedOAuthRequest.isPresent()) {
            response.setStatus(401);
            return;
        }
        RequestToken requestToken = new RequestToken(
                oauthToken,
                storedOAuthRequest.get().getSecret()
        );
        ResponseWriter responseWriter = writerResolver.writerFor(request, response);

        try {
            AccessToken token = twitter.getOAuthAccessToken(requestToken, oauthVerifier);

            // Twitter oauth tokens do not expire so null gets passed as expiry date see: https://dev.twitter.com/docs/faq
            Maybe<UserRef> userRef = accessTokenProcessor.process(new AuthToken(
                    token.getToken(),
                    token.getTokenSecret(),
                    UserNamespace.TWITTER,
                    null
            ));
            OAuthResult oauthResult;
            if (userRef.hasValue()) {
                // Make sure we have a user 
                updateUser(twitter, userRef.requireValue());
                oauthResult = OAuthResult.builder()
                        .withSuccess(true)
                        .withProvider(OAuthProvider.TWITTER)
                        .withToken(token.getToken())
                        .build();
            } else {
                oauthResult = OAuthResult.builder()
                        .withSuccess(false)
                        .withProvider(OAuthProvider.TWITTER)
                        .withToken("")
                        .build();
            }

            QueryResult<OAuthResult> queryResult = QueryResult.singleResult(
                    oauthResult,
                    QueryContext.standard(request)
            );
            oauthResultResultWriter.write(queryResult, responseWriter);
        } catch (TwitterException e) {
            throw new RuntimeException(e);
        }
    }

    private void updateUser(Twitter twitter, UserRef userRef) throws TwitterException {
        TwitterUserDetails twitterUserDetails = getUserDetails(twitter, userRef);
        User user = userStore.userForRef(userRef).or(userSupplier);
        if (user.getUserRef() == null) {
            user = user.copy().withUserRef(userRef).build();
        }
        if (user.getProfileImage() == null) {
            // Default personal information to that supplied by twitter
            user = user.copy()
                    .withScreenName(twitterUserDetails.getScreenName())
                    .withFullName(twitterUserDetails.getFullName())
                    .withProfileImage(twitterUserDetails.getProfileImage())
                    .withWebsite(twitterUserDetails.getHomepageUrl())
                    .withProfileComplete(false)
                    .build();
            userStore.store(user);
        }
    }

    private TwitterUserDetails getUserDetails(Twitter twitter, UserRef userRef)
            throws TwitterException {
        TwitterUserDetails userDetails = new TwitterUserDetails(userRef);

        long userIds[] = new long[] { Long.valueOf(userRef.getUserId()) };
        ResponseList<twitter4j.User> lookupResult = twitter.lookupUsers(userIds);
        if (!lookupResult.isEmpty()) {
            twitter4j.User twUser = lookupResult.get(0);
            String homepageUrl = twUser.getURLEntity().getExpandedURL();
            if (homepageUrl == null) {
                homepageUrl = twUser.getURLEntity().getURL();
            }
            userDetails.withProfileImage(twUser.getProfileImageURLHttps());
            userDetails.withHomepageUrl(homepageUrl);
            userDetails.withScreenName(twUser.getScreenName());
            userDetails.withFullName(twUser.getName());
        }
        return userDetails;
    }

}
