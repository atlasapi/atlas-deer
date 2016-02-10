package org.atlasapi.application;

import org.atlasapi.entity.Id;

public interface EndUserLicenseStore {

    EndUserLicense getById(Id id);

    void store(EndUserLicense license);
}
