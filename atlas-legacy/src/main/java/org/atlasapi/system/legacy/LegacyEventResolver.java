package org.atlasapi.system.legacy;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.atlasapi.entity.Id;
import org.atlasapi.entity.util.Resolved;
import org.atlasapi.event.Event;
import org.atlasapi.event.EventResolver;
import org.atlasapi.persistence.event.EventStore;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import static com.google.common.base.Preconditions.checkNotNull;

public class LegacyEventResolver implements EventResolver {

    private final org.atlasapi.persistence.event.EventResolver eventResolver;
    private final LegacyEventTransformer legacyEventTransformer;

    public LegacyEventResolver(EventStore eventResolver) {
        this(eventResolver, new LegacyEventTransformer());
    }

    @VisibleForTesting
    LegacyEventResolver(org.atlasapi.persistence.event.EventResolver eventResolver,
            LegacyEventTransformer legacyEventTransformer) {
        this.eventResolver = checkNotNull(eventResolver);
        this.legacyEventTransformer = checkNotNull(legacyEventTransformer);
    }

    @Override
    public ListenableFuture<Resolved<Event>> resolveIds(Iterable<Id> ids) {
        List<Event> events = StreamSupport.stream(ids.spliterator(), false)
                .map(Id::longValue)
                .map(eventResolver::fetch)
                .filter(Optional::isPresent)
                .map(Optional::get)
                .map(legacyEventTransformer::apply)
                .collect(Collectors.toList());

        return Futures.immediateFuture(Resolved.valueOf(events));
    }
}
