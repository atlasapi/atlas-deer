package org.atlasapi.application.model.auth;

import java.net.MalformedURLException;
import java.net.URL;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.metabroadcast.common.social.model.UserRef.UserNamespace;


public class OAuthRequest {
    private final URL authUrl;
    private final UserNamespace namespace;
    private final String token;
    private final String secret;
    
    private OAuthRequest(URL authUrl, UserNamespace namespace, String token, String secret) {
        Preconditions.checkNotNull(authUrl);
        Preconditions.checkNotNull(namespace);
        Preconditions.checkNotNull(token);
        Preconditions.checkNotNull(secret);
        this.authUrl = authUrl;
        this.namespace = namespace;
        this.token = token;
        this.secret = secret;
    }
    
    public URL getAuthUrl() {
        return authUrl;
    }
    
    public UserNamespace getUserNamespace() {
        return namespace;
    }
    
    public String getToken() {
        return token;
    }
    
    /**
     * This is the secret part of the Oauth token pair.
     * This should not be output in a ListWriter object 
     * as the secret portion should not be passed from
     * the web app to Atlas.
     * @return
     */
    public String getSecret() {
        return secret;
    }
    public String toString() {
        return Objects.toStringHelper(getClass())
                .add("authUrl", this.getAuthUrl())
                .add("namespace", this.getUserNamespace())
                .add("token", this.getToken())
                .add("secret", this.getSecret())
                .toString();
    }
    
    public static Builder builder() {
        return new Builder();
    }
    
    public static class Builder {
        private URL authUrl;
        private UserNamespace namespace;
        private String token;
        private String secret;
        
        public Builder withAuthUrl(URL authUrl) {
            this.authUrl = authUrl;
            return this;
        }
        
        public Builder withNamespace(UserNamespace namespace) {
            this.namespace = namespace;
            return this;
        }
        
        public Builder withToken(String token) {
            this.token = token;
            return this;
        }
        
        public Builder withSecret(String secret) {
            this.secret = secret;
            return this;
        }
        
        public Builder withAuthUrl(String authUrl) throws MalformedURLException {
            this.authUrl = new URL(authUrl);
            return this;
        }
        
        public OAuthRequest build() {
            return new OAuthRequest(this.authUrl, this.namespace, this.token, this.secret);
        }
    }
}
