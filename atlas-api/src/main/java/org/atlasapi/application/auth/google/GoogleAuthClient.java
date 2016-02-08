package org.atlasapi.application.auth.google;

import java.io.IOException;
import java.util.Collection;

import com.metabroadcast.common.social.auth.credentials.AuthToken;
import com.metabroadcast.common.social.model.UserRef.UserNamespace;

import com.google.api.client.auth.oauth2.Credential;
import com.google.api.client.googleapis.auth.oauth2.GoogleAuthorizationCodeRequestUrl;
import com.google.api.client.googleapis.auth.oauth2.GoogleAuthorizationCodeTokenRequest;
import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.auth.oauth2.GoogleTokenResponse;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.oauth2.Oauth2;
import com.google.api.services.oauth2.model.Userinfo;

import static com.google.api.client.repackaged.com.google.common.base.Preconditions.checkNotNull;

public class GoogleAuthClient {

    private static final String OAUTH_APPROVAL_PROMPT_FORCE = "force";
    private static final String OAUTH_ACCESS_TYPE_OFFLINE = "offline";
    private static final String ATLAS_APP_NAME = "Atlas";
    private final String consumerKey;
    private final String consumerSecret;
    private final JsonFactory jsonFactory = new JacksonFactory();
    private final HttpTransport httpTransport = new NetHttpTransport();

    public GoogleAuthClient(String consumerKey, String consumerSecret) {
        super();
        this.consumerKey = checkNotNull(consumerKey);
        this.consumerSecret = checkNotNull(consumerSecret);
    }

    public Userinfo getUserForToken(AuthToken accessToken) throws IOException {
        Credential credential = getCredentialForToken(accessToken);
        Oauth2 oauth2 = new Oauth2.Builder(httpTransport, jsonFactory, credential)
                .setApplicationName(ATLAS_APP_NAME)
                .build();
        return oauth2.userinfo().get().execute();
    }

    public String getAuthorizationCodeRequestUrl(Collection<String> scopes, String callbackUrl,
            String state) {
        return new GoogleAuthorizationCodeRequestUrl(consumerKey,
                callbackUrl, scopes
        )
                .setState(state)
                .setAccessType(OAUTH_ACCESS_TYPE_OFFLINE)
                .setApprovalPrompt(OAUTH_APPROVAL_PROMPT_FORCE)
                .build();
    }

    public AuthToken getAuthTokenForCode(String code, String redirectUri) throws IOException {
        GoogleTokenResponse tokenResponse = new GoogleAuthorizationCodeTokenRequest(
                httpTransport,
                jsonFactory,
                consumerKey,
                consumerSecret,
                code,
                redirectUri
        )
                .execute();
        return new AuthToken(tokenResponse.getAccessToken(), "", UserNamespace.GOOGLE, null);
    }

    private Credential getCredentialForToken(AuthToken token) {
        return new GoogleCredential.Builder()
                .setJsonFactory(jsonFactory)
                .setTransport(httpTransport)
                .setClientSecrets(consumerKey, consumerSecret).build()
                .setAccessToken(token.tokenPayload());
    }
}
