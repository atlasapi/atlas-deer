package org.atlasapi.system.legacy;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.atlasapi.entity.Id;
import org.atlasapi.entity.util.Resolved;
import org.atlasapi.eventV2.EventV2;
import org.atlasapi.eventV2.EventV2Resolver;
import org.atlasapi.organisation.OrganisationStore;
import org.atlasapi.persistence.event.EventStore;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import static com.google.common.base.Preconditions.checkNotNull;

public class LegacyEventV2Resolver implements EventV2Resolver {

    private final org.atlasapi.persistence.event.EventResolver eventResolver;
    private final LegacyEventV2Transformer legacyEventTransformer;

    public LegacyEventV2Resolver(EventStore eventResolver, OrganisationStore store) {
        this(eventResolver, new LegacyEventV2Transformer(store));
    }

    @VisibleForTesting
    LegacyEventV2Resolver(org.atlasapi.persistence.event.EventResolver eventResolver,
            LegacyEventV2Transformer legacyEventTransformer) {
        this.eventResolver = checkNotNull(eventResolver);
        this.legacyEventTransformer = checkNotNull(legacyEventTransformer);
    }

    @Override
    public ListenableFuture<Resolved<EventV2>> resolveIds(Iterable<Id> ids) {
        List<EventV2> events = StreamSupport.stream(ids.spliterator(), false)
                .map(Id::longValue)
                .map(eventResolver::fetch)
                .filter(Optional::isPresent)
                .map(Optional::get)
                .map(legacyEventTransformer::apply)
                .collect(Collectors.toList());

        return Futures.immediateFuture(Resolved.valueOf(events));
    }

}
