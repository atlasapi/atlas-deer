package org.atlasapi.organisation;

import org.atlasapi.entity.Id;

import com.google.common.base.Optional;
import com.google.common.util.concurrent.ListenableFuture;

public interface OrganisationUriResolver {

    ListenableFuture<Optional<Id>> getExistingId(Organisation organisation);

}
