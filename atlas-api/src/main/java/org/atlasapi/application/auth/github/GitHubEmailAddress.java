package org.atlasapi.application.auth.github;

import com.google.api.client.repackaged.com.google.common.base.Objects;
import com.google.api.client.util.Key;

public class GitHubEmailAddress {

    @Key("email")
    private String email;

    @Key("primary")
    private Boolean primary;

    @Key("verified")
    private Boolean verified;

    public String getEmail() {
        return email;
    }

    public Boolean getPrimary() {
        return primary;
    }

    public Boolean getVerified() {
        return verified;
    }

    public String toString() {
        return Objects.toStringHelper(getClass())
                .add("email", this.getEmail())
                .add("primary", this.getPrimary())
                .add("verified", this.getVerified())
                .toString();
    }
}
