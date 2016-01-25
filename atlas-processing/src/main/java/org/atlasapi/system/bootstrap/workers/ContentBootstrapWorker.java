package org.atlasapi.system.bootstrap.workers;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.concurrent.ExecutionException;

import org.atlasapi.content.Content;
import org.atlasapi.content.ContentResolver;
import org.atlasapi.content.ContentWriter;
import org.atlasapi.entity.Id;
import org.atlasapi.entity.util.Resolved;
import org.atlasapi.entity.util.WriteResult;
import org.atlasapi.messaging.ResourceUpdatedMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Timer;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Futures;
import com.metabroadcast.common.queue.RecoverableException;
import com.metabroadcast.common.queue.Worker;

public class ContentBootstrapWorker implements Worker<ResourceUpdatedMessage> {

    private static final Logger LOG = LoggerFactory.getLogger(ContentBootstrapWorker.class);

    private final ContentResolver contentResolver;
    private final ContentWriter writer;
    private final Timer metricsTimer;

    public ContentBootstrapWorker(ContentResolver contentResolver, ContentWriter writer,
            Timer metricsTimer) {
        this.contentResolver = checkNotNull(contentResolver);
        this.writer = checkNotNull(writer);
        this.metricsTimer = checkNotNull(metricsTimer);
    }

    @Override
    public void process(ResourceUpdatedMessage message) throws RecoverableException {
        Id contentId = message.getUpdatedResource().getId();
        LOG.debug("Processing message on id {}, message: {}", contentId, message);

        Timer.Context time = metricsTimer.time();
        Resolved<Content> content = resolveContent(contentId);

        try {
            WriteResult<Content, Content> result =
                    writer.writeContent(content.getResources().first().get());
            LOG.debug("Bootstrapped content {}", result.toString());
            time.stop();
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
}
