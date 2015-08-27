package org.atlasapi.system.bootstrap.workers;

import org.atlasapi.content.Content;
import org.atlasapi.content.ContentResolver;
import org.atlasapi.content.ContentStore;
import org.atlasapi.content.ContentWriter;
import org.atlasapi.entity.Alias;
import org.atlasapi.entity.Id;
import org.atlasapi.entity.util.Resolved;
import org.atlasapi.entity.util.WriteException;
import org.atlasapi.entity.util.WriteResult;
import org.atlasapi.media.entity.Publisher;

import com.google.common.util.concurrent.ListenableFuture;
import com.metabroadcast.common.collect.OptionalMap;

public final class DelegatingContentStore implements ContentStore {

    private final ContentResolver resolver;
    private final ContentWriter writer;

    public DelegatingContentStore(ContentResolver resolver, ContentWriter writer) {
        this.resolver = resolver;
        this.writer = writer;
    }

    @Override
    public OptionalMap<Alias, Content> resolveAliases(Iterable<Alias> aliases,
            Publisher source) {
        return resolver.resolveAliases(aliases, source);
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
}