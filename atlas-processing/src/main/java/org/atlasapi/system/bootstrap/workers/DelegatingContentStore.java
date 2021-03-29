package org.atlasapi.system.bootstrap.workers;

import org.atlasapi.content.Broadcast;
import org.atlasapi.content.ContainerRef;
import org.atlasapi.content.Content;
import org.atlasapi.content.ContentResolver;
import org.atlasapi.content.ContentStore;
import org.atlasapi.content.ContentWriter;
import org.atlasapi.content.ItemRef;
import org.atlasapi.content.SeriesRef;
import org.atlasapi.entity.Id;
import org.atlasapi.entity.util.Resolved;
import org.atlasapi.entity.util.WriteException;
import org.atlasapi.entity.util.WriteResult;

import com.google.common.base.Optional;
import com.google.common.util.concurrent.ListenableFuture;

public final class DelegatingContentStore implements ContentStore {

    private final ContentResolver resolver;
    private final ContentWriter writer;

    public DelegatingContentStore(ContentResolver resolver, ContentWriter writer) {
        this.resolver = resolver;
        this.writer = writer;
    }

    @Override
    public ListenableFuture<Resolved<Content>> resolveIds(Iterable<Id> ids) {
        return resolver.resolveIds(ids);
    }

    @Override
    public <C extends Content> WriteResult<C, Content> writeContent(C content)
            throws WriteException {
        return writer.writeContent(content);
    }

    @Override
    public <C extends Content> WriteResult<C, Content> forceWriteContent(C content)
            throws WriteException {
        return writer.forceWriteContent(content);
    }

    @Override
    public void writeBroadcast(
            ItemRef item,
            Optional<ContainerRef> containerRef,
            Optional<SeriesRef> seriesRef,
            Broadcast broadcast
    ) {
        writer.writeBroadcast(
                item,
                containerRef,
                seriesRef,
                broadcast
        );
    }
}