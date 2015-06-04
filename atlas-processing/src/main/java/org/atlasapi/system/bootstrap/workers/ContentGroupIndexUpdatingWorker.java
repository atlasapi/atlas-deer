package org.atlasapi.system.bootstrap.workers;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.IOException;

import org.atlasapi.content.ContentGroup;
import org.atlasapi.content.ContentGroupResolver;
import org.atlasapi.content.EsContentIndex;
import org.atlasapi.content.IndexException;
import org.atlasapi.entity.ResourceRef;
import org.atlasapi.entity.util.Resolved;
import org.atlasapi.messaging.ResourceUpdatedMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Futures;
import com.metabroadcast.common.queue.RecoverableException;
import com.metabroadcast.common.queue.Worker;

public class ContentGroupIndexUpdatingWorker implements Worker<ResourceUpdatedMessage> {

    private static final Logger log = LoggerFactory.getLogger(ContentGroupIndexUpdatingWorker.class);

    private final EsContentIndex index;
    private final ContentGroupResolver resolver;

    public ContentGroupIndexUpdatingWorker(EsContentIndex index, ContentGroupResolver resolver) {
        this.index = checkNotNull(index);
        this.resolver = checkNotNull(resolver);
    }

    @Override
    public void process(ResourceUpdatedMessage msg) throws RecoverableException {
        try {
            log.debug("Starting processing of content group update {}", msg.getUpdatedResource());
            ResourceRef resource = msg.getUpdatedResource();
            Resolved<ContentGroup> resolved =
                    Futures.get(resolver.resolveIds(ImmutableList.of(resource.getId())), IOException.class);

            Optional<ContentGroup> maybeContentGroup = resolved.getResources().first();
            if (!maybeContentGroup.isPresent()) {
                log.warn(
                        "Could not resolve count group {} in legacy store, skipping update",
                        msg.getUpdatedResource().getId()
                );
                return;
            }


            ContentGroup contentGroup = maybeContentGroup.get();
            if (contentGroup.getId() == null) {
                log.warn("Cannot index content group with no id - {}", contentGroup.toString());
            }

            index.index(contentGroup);
            log.debug("Finished processing of content group update {}", msg.getUpdatedResource());
        } catch (IOException | IndexException e) {
            log.error(
                    "Failed to process update message for content group {} due to {}",
                    msg.getUpdatedResource().getId(),
                    e.toString()
            );
        }
    }
}
