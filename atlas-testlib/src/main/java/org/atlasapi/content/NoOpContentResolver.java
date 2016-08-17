package org.atlasapi.content;

import org.atlasapi.entity.Id;
import org.atlasapi.entity.util.Resolved;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

public class NoOpContentResolver implements ContentResolver {

    @Override
    public ListenableFuture<Resolved<Content>> resolveIds(Iterable<Id> ids) {
        return Futures.immediateFuture(Resolved.valueOf(ImmutableList.of()));
    }
}
