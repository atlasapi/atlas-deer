package org.atlasapi.application.model.auth;

import com.google.common.base.Preconditions;

public class OAuthResult {

    public final boolean success;
    public final OAuthProvider provider;
    public final String accessToken;

    private OAuthResult(boolean success, OAuthProvider provider, String token) {
        Preconditions.checkNotNull(provider);
        Preconditions.checkNotNull(token);
        this.success = success;
        this.provider = provider;
        this.accessToken = token;
    }

    public boolean isSuccess() {
        return success;
    }

    public OAuthProvider getProvider() {
        return this.provider;
    }

    public String getAccessToken() {
        return accessToken;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {

        public boolean success;
        public OAuthProvider provider;
        public String accessToken;

        public boolean isSuccess() {
            return success;
        }

        public OAuthProvider getProvider() {
            return this.provider;
        }

        public String getAccessToken() {
            return accessToken;
        }

        public Builder withSuccess(boolean success) {
            this.success = success;
            return this;
        }

        public Builder withProvider(OAuthProvider provider) {
            this.provider = provider;
            return this;
        }

        public Builder withToken(String token) {
            this.accessToken = token;
            return this;
        }

        public OAuthResult build() {
            return new OAuthResult(this.isSuccess(), this.getProvider(), this.getAccessToken());
        }
    }
}
