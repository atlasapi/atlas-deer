package org.atlasapi.application;

import org.atlasapi.media.entity.Publisher;

public class SourceLicense {

    private final Publisher source;
    private final String license;

    private SourceLicense(Publisher source, String license) {
        super();
        this.source = source;
        this.license = license;
    }

    public Publisher getSource() {
        return source;
    }

    public String getLicense() {
        return license;
    }

    public static SourceLicenseBuilder builder() {
        return new SourceLicenseBuilder();
    }

    public static class SourceLicenseBuilder {

        private Publisher source;
        private String license;

        public SourceLicenseBuilder withSource(Publisher source) {
            this.source = source;
            return this;
        }

        public SourceLicenseBuilder withLicense(String license) {
            this.license = license;
            return this;
        }

        public SourceLicense build() {
            return new SourceLicense(source, license);
        }
    }

}
