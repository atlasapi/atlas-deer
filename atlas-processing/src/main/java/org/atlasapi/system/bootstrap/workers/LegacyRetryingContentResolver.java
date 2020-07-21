package org.atlasapi.system.bootstrap.workers;

import java.io.IOException;

import org.atlasapi.content.Content;
import org.atlasapi.content.ContentResolver;
import org.atlasapi.content.ContentWriter;
import org.atlasapi.entity.Id;
import org.atlasapi.entity.util.Resolved;
import org.atlasapi.entity.util.WriteException;
import org.atlasapi.system.legacy.LegacyContentResolver;

import com.metabroadcast.common.collect.OptionalMap;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import static com.google.common.base.Preconditions.checkNotNull;

public class LegacyRetryingContentResolver implements ContentResolver {

    private final ContentResolver currentResolver;
    private final LegacyContentResolver legacyResolver;
    private final ContentWriter writer;

    public LegacyRetryingContentResolver(
            ContentResolver currentResolver,
            LegacyContentResolver legacyResolver,
            ContentWriter writer
    ) {
        this.currentResolver = checkNotNull(currentResolver);
        this.legacyResolver = checkNotNull(legacyResolver);
        this.writer = checkNotNull(writer);
    }

    @Override
    public ListenableFuture<Resolved<Content>> resolveIds(Iterable<Id> ids) {
        try {
            Resolved<Content> currentResolved = Futures.getChecked(
                    currentResolver.resolveIds(ids),
                    IOException.class
            );

            if (hasAllIds(currentResolved, ids)) {
                return Futures.immediateFuture(currentResolved);
            }
            Iterable<Id> missingIds = missingIds(currentResolved, ids);

            Resolved<Content> legacyResolved = Futures.getChecked(
                    legacyResolver.resolveIds(missingIds),
                    IOException.class
            );
            for (Content legacyContent : legacyResolved.getResources()) {
                writer.writeContent(legacyContent);
            }
            return Futures.immediateFuture(
                    Resolved.valueOf(
                            ImmutableList.<Content>builder()
                                    .addAll(currentResolved.getResources())
                                    .addAll(legacyResolved.getResources())
                                    .build()
                    )
            );

        } catch (IOException | WriteException e) {
            throw Throwables.propagate(e);
        }
    }

    private Boolean hasAllIds(Resolved<Content> resolved, Iterable<Id> ids) {
        OptionalMap<Id, Content> contentMap = resolved.toMap();
        for (Id id : ids) {
            if (!contentMap.get(id).isPresent()) {
                return false;
            }
        }
        return true;
    }

    private Iterable<Id> missingIds(Resolved<Content> resolved, Iterable<Id> ids) {
        ImmutableList.Builder<Id> missingIds = ImmutableList.builder();

        OptionalMap<Id, Content> contentMap = resolved.toMap();
        for (Id id : ids) {
            if (!contentMap.get(id).isPresent()) {
                missingIds.add(id);
            }
        }
        return missingIds.build();
    }

}
