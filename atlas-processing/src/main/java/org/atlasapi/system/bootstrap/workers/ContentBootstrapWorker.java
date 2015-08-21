package org.atlasapi.system.bootstrap.workers;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Futures;
import com.metabroadcast.common.queue.Worker;
import org.atlasapi.content.Content;
import org.atlasapi.content.ContentResolver;
import org.atlasapi.content.ContentWriter;
import org.atlasapi.entity.Id;
import org.atlasapi.entity.util.Resolved;
import org.atlasapi.entity.util.WriteResult;
import org.atlasapi.messaging.ResourceUpdatedMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutionException;

import static com.google.common.base.Preconditions.checkNotNull;

public class ContentBootstrapWorker implements Worker<ResourceUpdatedMessage> {

    private final Logger log = LoggerFactory.getLogger(ContentBootstrapWorker.class);

    private final ContentResolver contentResolver;
    private final ContentWriter writer;
    private final Timer messagesTimer;

    public ContentBootstrapWorker(ContentResolver contentResolver, ContentWriter writer, MetricRegistry metricsRegistry) {
        this.contentResolver = checkNotNull(contentResolver);
        this.writer = checkNotNull(writer);
        this.messagesTimer = (metricsRegistry != null ? checkNotNull(metricsRegistry.timer("ContentBootstrapWorker")) : null);
    }

    @Override
    public void process(ResourceUpdatedMessage message) {
        try {
            if (messagesTimer != null) {
                Id contentId = message.getUpdatedResource().getId();
                Resolved<Content> content = Futures.get(
                        contentResolver.resolveIds(ImmutableList.of(contentId)),
                        ExecutionException.class
                );
                WriteResult<Content, Content> result = writer.writeContent(content.getResources().first().get());
                log.debug("Bootstrapped content {}", result.toString());
            }
        } catch (Exception e) {
            log.error("Failed to bootstrap content {}", message.getUpdatedResource());
        }
    }
}