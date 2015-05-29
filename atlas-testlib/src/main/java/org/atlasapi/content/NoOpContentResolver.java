package org.atlasapi.content;

import org.atlasapi.entity.Alias;
import org.atlasapi.entity.Id;
import org.atlasapi.entity.util.Resolved;
import org.atlasapi.media.entity.Publisher;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.metabroadcast.common.collect.OptionalMap;

public class NoOpContentResolver implements ContentResolver {

    @Override public OptionalMap<Alias, Content> resolveAliases(Iterable<Alias> aliases,
            Publisher source) {
        return null;
    }

    @Override public ListenableFuture<Resolved<Content>> resolveIds(Iterable<Id> ids) {
        return Futures.immediateFuture(Resolved.valueOf(ImmutableList.of()));
    }
}
