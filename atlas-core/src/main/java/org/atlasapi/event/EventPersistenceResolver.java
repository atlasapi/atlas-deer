package org.atlasapi.event;

import org.atlasapi.entity.Alias;
import org.atlasapi.entity.Id;
import org.atlasapi.entity.util.Resolved;
import org.atlasapi.media.entity.Publisher;

import com.google.common.base.Optional;
import com.google.common.util.concurrent.ListenableFuture;

public interface EventPersistenceResolver {

    ListenableFuture<Resolved<Event>> resolveIds(Iterable<Id> ids);

    Optional<Event> resolvePrevious(Optional<Id> id, Publisher source,
            Iterable<Alias> aliases);

}
