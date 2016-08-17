package org.atlasapi.content;

import org.atlasapi.entity.Id;
import org.atlasapi.entity.util.Resolved;
import org.atlasapi.entity.util.WriteException;
import org.atlasapi.entity.util.WriteResult;

import com.google.common.util.concurrent.ListenableFuture;

public abstract class ForwardingContentStore implements ContentStore {

    protected ForwardingContentStore() {
    }

    protected abstract ContentStore delegate();

    @Override
    public ListenableFuture<Resolved<Content>> resolveIds(Iterable<Id> ids) {
        return delegate().resolveIds(ids);
    }

    @Override
    public <C extends Content> WriteResult<C, Content> writeContent(C content)
            throws WriteException {
        return delegate().writeContent(content);
    }

}
