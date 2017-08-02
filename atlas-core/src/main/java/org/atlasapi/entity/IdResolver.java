package org.atlasapi.entity;

import org.atlasapi.entity.util.Resolved;

import com.google.common.util.concurrent.ListenableFuture;

public interface IdResolver<I extends Identifiable> {

    ListenableFuture<Resolved<I>> resolveIds(Iterable<Id> ids);

    default ListenableFuture<Resolved<I>> resolveIds(Iterable<Id> ids, Boolean refreshCache) {
        throw new UnsupportedOperationException();
    }

}
