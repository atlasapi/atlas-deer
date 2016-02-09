package org.atlasapi.content;

import org.atlasapi.entity.Alias;
import org.atlasapi.entity.Id;
import org.atlasapi.entity.util.Resolved;
import org.atlasapi.media.entity.Publisher;

import com.metabroadcast.common.collect.ImmutableOptionalMap;
import com.metabroadcast.common.collect.OptionalMap;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

public class NullContentResolver implements ContentResolver {

    private static final ContentResolver INSTANCE = new NullContentResolver();

    public static ContentResolver get() {
        return INSTANCE;
    }

    private NullContentResolver() {
    }

    @Override
    public ListenableFuture<Resolved<Content>> resolveIds(Iterable<Id> ids) {
        return Futures.immediateFuture(Resolved.<Content>empty());
    }

    @Override
    public OptionalMap<Alias, Content> resolveAliases(Iterable<Alias> aliases, Publisher source) {
        return ImmutableOptionalMap.of();
    }

}
