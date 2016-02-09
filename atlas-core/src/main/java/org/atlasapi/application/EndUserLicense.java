package org.atlasapi.application;

import org.atlasapi.entity.Id;

public class EndUserLicense {

    private final Id id;
    private final String license;

    public EndUserLicense(Id id, String license) {
        this.id = id;
        this.license = license;
    }

    public Id getId() {
        return id;
    }

    public String getLicense() {
        return license;
    }

    public Builder copy() {
        return builder()
                .withId(this.getId())
                .withLicense(this.getLicense());
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {

        private Id id;
        private String license;

        public Builder withId(Id id) {
            this.id = id;
            return this;
        }

        public Builder withLicense(String license) {
            this.license = license;
            return this;
        }

        public EndUserLicense build() {
            return new EndUserLicense(id, license);
        }
    }
}
