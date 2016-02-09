package org.atlasapi.users.videosource.model;

import com.google.api.client.repackaged.com.google.common.base.Preconditions;
import com.google.common.base.Objects;

public class OauthToken {

    private final String accessToken;
    private final String tokenType;
    private final Long expiresIn;
    private final String idToken;
    private final String refreshToken;

    private OauthToken(String accessToken, String tokenType,
            Long expiresIn, String idToken, String refreshToken) {
        this.accessToken = accessToken;
        this.tokenType = tokenType;
        this.expiresIn = expiresIn;
        this.idToken = idToken;
        this.refreshToken = refreshToken;
    }

    public String getAccessToken() {
        return accessToken;
    }

    public String getTokenType() {
        return tokenType;
    }

    public Long getExpiresIn() {
        return expiresIn;
    }

    public String getIdToken() {
        return idToken;
    }

    public String getRefreshToken() {
        return refreshToken;
    }

    public String toString() {
        return Objects.toStringHelper(getClass())
                .add("accessToken", this.getAccessToken())
                .add("tokenType", this.getTokenType())
                .add("expiresIn", this.getExpiresIn())
                .add("idToken", this.getIdToken())
                .add("refreshToken", this.getRefreshToken())
                .toString();
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {

        private String accessToken;
        private String tokenType;
        private Long expiresIn;
        private String idToken;
        private String refreshToken;

        public Builder withAccessToken(String accessToken) {
            this.accessToken = accessToken;
            return this;
        }

        public Builder withTokenType(String tokenType) {
            this.tokenType = tokenType;
            return this;
        }

        public Builder withExpiresIn(Long expiresIn) {
            this.expiresIn = expiresIn;
            return this;
        }

        public Builder withIdToken(String idToken) {
            this.idToken = idToken;
            return this;
        }

        public Builder withRefreshToken(String refreshToken) {
            this.refreshToken = refreshToken;
            return this;
        }

        public OauthToken build() {
            Preconditions.checkNotNull(accessToken);
            Preconditions.checkNotNull(tokenType);
            Preconditions.checkNotNull(expiresIn);
            Preconditions.checkNotNull(idToken);
            Preconditions.checkNotNull(refreshToken);
            return new OauthToken(accessToken, tokenType,
                    expiresIn, idToken, refreshToken
            );
        }
    }

}
