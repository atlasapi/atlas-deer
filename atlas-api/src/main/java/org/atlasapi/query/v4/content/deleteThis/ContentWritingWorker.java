package org.atlasapi.query.v4.content.deleteThis;

import java.util.concurrent.ExecutionException;

import org.atlasapi.content.Content;
import org.atlasapi.content.ContentResolver;
import org.atlasapi.content.ContentWriter;
import org.atlasapi.entity.Id;
import org.atlasapi.entity.util.Resolved;
import org.atlasapi.entity.util.WriteResult;
import org.atlasapi.messaging.ResourceUpdatedMessage;

import com.metabroadcast.common.queue.AbstractMessage;
import com.metabroadcast.common.queue.RecoverableException;
import com.metabroadcast.common.queue.Worker;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Futures;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkNotNull;

public class ContentWritingWorker implements Worker<ResourceUpdatedMessage> {

    private static final Logger LOG = LoggerFactory.getLogger(ContentWritingWorker.class);

    private final ContentResolver contentResolver;
    private final ContentWriter writer;

    public ContentWritingWorker(ContentResolver contentResolver, ContentWriter writer) {
        this.contentResolver = checkNotNull(contentResolver);
        this.writer = checkNotNull(writer);
    }

    @Override
    public void process(ResourceUpdatedMessage message) throws RecoverableException {
        Id contentId = message.getUpdatedResource().getId();
        LOG.debug("Processing message on id {}, took: PT{}S, message: {}",
                contentId, getTimeToProcessInSeconds(message), message);

        Resolved<Content> content = resolveContent(contentId);

        try {
            WriteResult<Content, Content> result =
                    writer.writeContent(content.getResources().first().get());
            LOG.debug("Bootstrapped content {}", result.toString());
        } catch (Exception e) {
            String errorMsg = "Failed to bootstrap content " + message.getUpdatedResource();
            LOG.error(errorMsg, e);
            throw Throwables.propagate(e);
        }
    }

    private Resolved<Content> resolveContent(Id contentId) {
        try {
            return Futures.get(
                    contentResolver.resolveIds(ImmutableList.of(contentId)),
                    ExecutionException.class
            );
        } catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }

    private long getTimeToProcessInSeconds(AbstractMessage message) {
        return (System.currentTimeMillis() - message.getTimestamp().millis()) / 1000L;
    }
}