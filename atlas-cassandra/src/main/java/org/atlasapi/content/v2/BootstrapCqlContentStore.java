package org.atlasapi.content.v2;

import org.atlasapi.content.Content;
import org.atlasapi.entity.util.WriteException;
import org.atlasapi.messaging.ResourceUpdatedMessage;

import com.google.common.collect.ImmutableList;

public class BootstrapCqlContentStore extends CqlContentStore {

    private BootstrapCqlContentStore(Builder builder) {
        super(builder);
    }

    public static BootstrapCqlContentStore create(CqlContentStore.Builder cqlBuilder) {
        return new BootstrapCqlContentStore(cqlBuilder);
    }

    @Override
    protected Content resolvePrevious(Content content) throws WriteException {
        return null;
    }

    @Override
    protected void sendMessages(ImmutableList<ResourceUpdatedMessage> messages) {
        // overridden to be a no-op because the base does a graph lookup and we don't need that
    }
}
