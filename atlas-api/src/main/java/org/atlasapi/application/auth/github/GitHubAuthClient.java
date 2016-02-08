package org.atlasapi.application.auth.github;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

import com.metabroadcast.common.social.auth.credentials.AuthToken;
import com.metabroadcast.common.social.model.UserRef.UserNamespace;

import com.google.api.client.auth.oauth2.AuthorizationCodeRequestUrl;
import com.google.api.client.auth.oauth2.AuthorizationCodeTokenRequest;
import com.google.api.client.auth.oauth2.ClientParametersAuthentication;
import com.google.api.client.auth.oauth2.TokenResponse;
import com.google.api.client.http.GenericUrl;
import com.google.api.client.http.HttpRequest;
import com.google.api.client.http.HttpRequestFactory;
import com.google.api.client.http.HttpResponse;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.JsonObjectParser;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.common.collect.ImmutableList;

import static com.google.api.client.repackaged.com.google.common.base.Preconditions.checkNotNull;

public class GitHubAuthClient {

    private static final String ACCESS_TOKEN_URL = "https://github.com/login/oauth/access_token";
    private static final String USER_INFO_URL = "https://api.github.com/user";
    private static final String USER_EMAIL_URL = "https://api.github.com/user/emails";
    private static final String AUTHORIZE_URL = "https://github.com/login/oauth/authorize";
    private final String githubConsumerKey;
    private final String githubConsumerSecret;
    private final JsonFactory jsonFactory = new JacksonFactory();
    private final HttpTransport httpTransport = new NetHttpTransport();

    public GitHubAuthClient(String githubConsumerKey, String githubConsumerSecret) {
        super();
        this.githubConsumerKey = checkNotNull(githubConsumerKey);
        this.githubConsumerSecret = checkNotNull(githubConsumerSecret);
    }

    public String getAuthorizationCodeRequestUrl(Collection<String> scopes, String callbackUrl) {
        return new AuthorizationCodeRequestUrl(AUTHORIZE_URL, githubConsumerKey)
                .setScopes(scopes)
                .setRedirectUri(callbackUrl)
                .build();
    }

    public AuthToken getAuthTokenForCode(String code) throws IOException {
        TokenResponse ghResponse =
                new AuthorizationCodeTokenRequest(httpTransport, jsonFactory,
                        new GenericUrl(ACCESS_TOKEN_URL),
                        code
                )
                        .setRequestInitializer(new AcceptJson())
                        .setClientAuthentication(
                                new ClientParametersAuthentication(
                                        githubConsumerKey,
                                        githubConsumerSecret
                                ))
                        .execute();
        return new AuthToken(ghResponse.getAccessToken(), "", UserNamespace.GITHUB, null);
    }

    public GitHubUser getUserForToken(AuthToken token) throws IOException {
        HttpRequestFactory requestFactory =
                httpTransport.createRequestFactory();
        // make request
        GenericUrl url = new GenericUrl(USER_INFO_URL);
        url.set("access_token", token.tokenPayload());
        HttpRequest request =
                requestFactory.buildGetRequest(url);
        request.setParser(new JsonObjectParser(jsonFactory));
        request.setThrowExceptionOnExecuteError(true);

        HttpResponse response = request.execute();
        return response.parseAs(GitHubUser.class);
    }

    public List<GitHubEmailAddress> getEmailsForToken(AuthToken token) throws IOException {
        HttpRequestFactory requestFactory =
                httpTransport.createRequestFactory(new GitHubV3MediaType());
        // make request
        GenericUrl url = new GenericUrl(USER_EMAIL_URL);
        url.set("access_token", token.tokenPayload());
        HttpRequest request = requestFactory.buildGetRequest(url);
        request.setParser(new JsonObjectParser(jsonFactory));
        request.setThrowExceptionOnExecuteError(true);
        HttpResponse response = request.execute();
        return ImmutableList.copyOf(response.parseAs(GitHubEmailAddress[].class));
    }

    public String getPrimaryEmailAddress(AuthToken token)
            throws IOException, IllegalStateException {
        List<GitHubEmailAddress> emails = getEmailsForToken(token);
        for (GitHubEmailAddress email : emails) {
            if (Boolean.TRUE.equals(email.getPrimary())) {
                return email.getEmail();
            }
        }
        throw new IllegalStateException("No primary email address found on GitHub");
    }
}
