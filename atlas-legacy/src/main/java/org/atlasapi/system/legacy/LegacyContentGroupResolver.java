package org.atlasapi.system.legacy;

import static com.google.common.base.Preconditions.checkNotNull;

import org.atlasapi.content.ContentGroup;
import org.atlasapi.content.ContentGroupResolver;
import org.atlasapi.entity.Id;
import org.atlasapi.entity.util.Resolved;
import org.atlasapi.persistence.content.ResolvedContent;
import org.atlasapi.persistence.content.mongo.MongoContentGroupResolver;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;

public class LegacyContentGroupResolver implements ContentGroupResolver {

    private final LegacyContentGroupTransformer transformer = new LegacyContentGroupTransformer();
    private final MongoContentGroupResolver legacyResolver;

    public LegacyContentGroupResolver(MongoContentGroupResolver legacyResolver) {
        this.legacyResolver = checkNotNull(legacyResolver);
    }

    @Override
    public ListenableFuture<Resolved<ContentGroup>> resolveIds(Iterable<Id> ids) {
        SettableFuture<Resolved<ContentGroup>> future = SettableFuture.create();
        ResolvedContent resolved = legacyResolver.findByIds(Iterables.transform(ids, Id::longValue));
        org.atlasapi.media.entity.ContentGroup legacyGroup =
                (org.atlasapi.media.entity.ContentGroup) resolved.getFirstValue().requireValue();
        future.set(Resolved.valueOf(ImmutableList.of(transformer.createDescribed(legacyGroup))));
        return future;
    }

}
