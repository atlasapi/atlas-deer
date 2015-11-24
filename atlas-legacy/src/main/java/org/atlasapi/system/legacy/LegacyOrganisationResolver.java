package org.atlasapi.system.legacy;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.atlasapi.entity.Id;
import org.atlasapi.entity.util.Resolved;
import org.atlasapi.organisation.Organisation;
import org.atlasapi.organisation.OrganisationResolver;
import org.atlasapi.persistence.content.organisation.MongoOrganisationStore;
import org.atlasapi.persistence.content.organisation.OrganisationStore;

import com.google.common.base.Optional;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

public class LegacyOrganisationResolver implements OrganisationResolver {
    private OrganisationStore store;
    private LegacyOrganisationTransformer transformer;

    public LegacyOrganisationResolver(MongoOrganisationStore store, LegacyOrganisationTransformer transformer) {
        this.store = store;
        this.transformer = transformer;
    }

    @Override public ListenableFuture<Resolved<Organisation>> resolveIds(
            Iterable<Id> ids) {
        List<Organisation> events = StreamSupport.stream(ids.spliterator(), false)
                .map(Id::longValue)
                .map(store::organisation)
                .filter(Optional::isPresent)
                .map(Optional::get)
                .map(transformer::apply)
                .collect(Collectors.toList());

        return Futures.immediateFuture(Resolved.valueOf(events));

    }
}
