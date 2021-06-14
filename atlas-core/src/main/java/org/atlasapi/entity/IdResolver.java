package org.atlasapi.entity;

import com.google.common.util.concurrent.ListenableFuture;
import org.atlasapi.entity.util.Resolved;

public interface IdResolver<I extends Identifiable> {

    ListenableFuture<Resolved<I>> resolveIds(Iterable<Id> ids);

    default ListenableFuture<Resolved<I>> resolveIds(Iterable<Id> ids, boolean refreshCache) {
        return resolveIds(ids);
    }

}
