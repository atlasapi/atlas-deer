package org.atlasapi.organisation;

import org.atlasapi.entity.Id;
import org.atlasapi.entity.util.Resolved;

import com.google.common.util.concurrent.ListenableFuture;

public interface OrganisationResolver {
    ListenableFuture<Resolved<Organisation>> resolveIds(Iterable<Id> ids);
}
