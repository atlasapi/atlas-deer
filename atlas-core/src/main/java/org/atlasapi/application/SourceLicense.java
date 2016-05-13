package org.atlasapi.application;

import javax.annotation.Nullable;

import org.atlasapi.media.entity.Publisher;

public class SourceLicense {

    private final Publisher source;
    private final String license;

    private SourceLicense(
            @Nullable Publisher source,
            @Nullable String license
    ) {
        super();
        this.source = source;
        this.license = license;
    }

    public static SourceLicenseBuilder builder() {
        return new SourceLicenseBuilder();
    }

    @Nullable
    public Publisher getSource() {
        return source;
    }

    @Nullable
    public String getLicense() {
        return license;
    }

    public static class SourceLicenseBuilder {

        private Publisher source;
        private String license;

        public SourceLicenseBuilder withSource(@Nullable Publisher source) {
            this.source = source;
            return this;
        }

        public SourceLicenseBuilder withLicense(@Nullable String license) {
            this.license = license;
            return this;
        }

        public SourceLicense build() {
            return new SourceLicense(source, license);
        }
    }
}
