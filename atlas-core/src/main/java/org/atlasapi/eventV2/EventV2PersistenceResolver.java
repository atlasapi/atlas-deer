package org.atlasapi.eventV2;

import org.atlasapi.entity.Alias;
import org.atlasapi.entity.Id;
import org.atlasapi.entity.util.Resolved;
import org.atlasapi.media.entity.Publisher;

import com.google.common.base.Optional;
import com.google.common.util.concurrent.ListenableFuture;

public interface EventV2PersistenceResolver {

    ListenableFuture<Resolved<EventV2>> resolveIds(Iterable<Id> ids);

    Optional<EventV2> resolvePrevious(Optional<Id> id, Publisher source,
            Iterable<Alias> aliases);

}
