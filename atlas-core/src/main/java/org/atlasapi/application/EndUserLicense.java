package org.atlasapi.application;

import javax.annotation.Nullable;

import org.atlasapi.entity.Id;

public class EndUserLicense {

    private final Id id;
    private final String license;

    private EndUserLicense(
            @Nullable Id id,
            @Nullable String license
    ) {
        this.id = id;
        this.license = license;
    }

    public static Builder builder() {
        return new Builder();
    }

    @Nullable
    public Id getId() {
        return id;
    }

    @Nullable
    public String getLicense() {
        return license;
    }

    public Builder copy() {
        return builder()
                .withId(this.getId())
                .withLicense(this.getLicense());
    }

    public static class Builder {

        private Id id;
        private String license;

        public Builder withId(@Nullable Id id) {
            this.id = id;
            return this;
        }

        public Builder withLicense(@Nullable String license) {
            this.license = license;
            return this;
        }

        public EndUserLicense build() {
            return new EndUserLicense(id, license);
        }
    }
}
