package org.atlasapi.application.model.auth;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.UUID;

import com.metabroadcast.common.social.model.UserRef.UserNamespace;

import com.google.common.base.Objects;

import static com.google.common.base.Preconditions.checkNotNull;

public class OAuthRequest {

    private final UUID uuid;
    private final URL authUrl;
    private final URL callbackUrl;
    private final UserNamespace namespace;
    private final String token;
    private final String secret;

    private OAuthRequest(UUID uuid, URL authUrl, URL callbackUrl, UserNamespace namespace,
            String token, String secret) {
        this.uuid = checkNotNull(uuid);
        this.authUrl = checkNotNull(authUrl);
        this.callbackUrl = checkNotNull(callbackUrl);
        this.namespace = checkNotNull(namespace);
        this.token = checkNotNull(token);
        this.secret = checkNotNull(secret);
    }

    public UUID getUuid() {
        return uuid;
    }

    public URL getAuthUrl() {
        return authUrl;
    }

    public URL getCallbackUrl() {
        return callbackUrl;
    }

    public UserNamespace getUserNamespace() {
        return namespace;
    }

    public String getToken() {
        return token;
    }

    /**
     * This is the secret part of the Oauth token pair. This should not be output in a ListWriter
     * object as the secret portion should not be passed from the web app to Atlas.
     *
     * @return
     */
    public String getSecret() {
        return secret;
    }

    public String toString() {
        return Objects.toStringHelper(getClass())
                .add("uuid", this.getUuid())
                .add("authUrl", this.getAuthUrl())
                .add("callbackUrl", this.getCallbackUrl())
                .add("namespace", this.getUserNamespace())
                .add("token", this.getToken())
                .add("secret", this.getSecret())
                .toString();
    }

    public static Builder builder() {
        return new Builder();
    }

    public static UUID generateUuid() {
        return UUID.randomUUID();
    }

    public static class Builder {

        private UUID uuid;
        private URL authUrl;
        private URL callbackUrl;
        private UserNamespace namespace;
        private String token;
        private String secret;

        public Builder withUuid(UUID uuid) {
            this.uuid = uuid;
            return this;
        }

        public Builder withUuid(String uuid) {
            return withUuid(UUID.fromString(uuid));
        }

        public Builder withAuthUrl(URL authUrl) {
            this.authUrl = authUrl;
            return this;
        }

        public Builder withAuthUrl(String authUrl) throws MalformedURLException {
            this.authUrl = new URL(authUrl);
            return this;
        }

        public Builder withCallbackUrl(URL callbackUrl) {
            this.callbackUrl = callbackUrl;
            return this;
        }

        public Builder withCallbackUrl(String callbackUrl) throws MalformedURLException {
            this.callbackUrl = new URL(callbackUrl);
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

        public OAuthRequest build() {
            return new OAuthRequest(uuid, authUrl, callbackUrl, namespace, token, secret);
        }
    }
}
