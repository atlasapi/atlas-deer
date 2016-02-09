package org.atlasapi.application;

import org.atlasapi.media.entity.Publisher;

import com.google.common.base.Optional;

public interface SourceLicenseStore {

    Optional<SourceLicense> licenseFor(Publisher source);

    void store(SourceLicense sourceLicense);
}
