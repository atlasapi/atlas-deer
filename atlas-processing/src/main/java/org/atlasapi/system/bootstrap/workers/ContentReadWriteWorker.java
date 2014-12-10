package org.atlasapi.system.bootstrap.workers;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Set;
import java.util.concurrent.ExecutionException;

import org.atlasapi.content.Content;
import org.atlasapi.content.ContentResolver;
import org.atlasapi.content.ContentWriter;
import org.atlasapi.entity.Id;
import org.atlasapi.entity.ResourceRef;
import org.atlasapi.entity.util.MissingResourceException;
import org.atlasapi.entity.util.Resolved;
import org.atlasapi.entity.util.WriteException;
import org.atlasapi.equivalence.EquivalenceGraphStore;
import org.atlasapi.media.entity.LookupRef;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.messaging.ResourceUpdatedMessage;
import org.atlasapi.persistence.lookup.entry.LookupEntry;
import org.atlasapi.persistence.lookup.entry.LookupEntryStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.metabroadcast.common.queue.Worker;

public class ContentReadWriteWorker implements Worker<ResourceUpdatedMessage> {

    private static final int maxAttempts = 3;
    private static final Function<Content, ResourceRef> TO_RESOURCE_REF = new Function<Content, ResourceRef>() {

        @Override
        public ResourceRef apply(Content input) {
            return input.toRef();
        }
    };
    private static final Function<ResourceRef, Publisher> TO_SOURCE = new Function<ResourceRef, Publisher>() {

        @Override
        public Publisher apply(ResourceRef input) {
            return input.getPublisher();
        }
    };

    private final Logger log = LoggerFactory.getLogger(ContentReadWriteWorker.class);

    private final ContentResolver contentResolver;
    private final ContentWriter writer;
    private final LookupEntryStore entryStore;
    private final EquivalenceGraphStore equivalenceGraphStore;

    public ContentReadWriteWorker(ContentResolver contentResolver, ContentWriter writer, LookupEntryStore entryStore,
                                  EquivalenceGraphStore equivalenceGraphStore) {
        this.contentResolver = contentResolver;
        this.writer = writer;
        this.entryStore = checkNotNull(entryStore);
        this.equivalenceGraphStore = checkNotNull(equivalenceGraphStore);
    }

    @Override
    public void process(ResourceUpdatedMessage message) {
        readAndWrite(message.getUpdatedResource().getId());
    }

    private void readAndWrite(Id id) {
        readAndWrite(id, 0);
    }

    private void readAndWrite(final Id id, final int attempt) {
        if (attempt >= maxAttempts) {
            throw new RuntimeException(String.format("Failed to write %s in %s attempts", id, maxAttempts));
        }
        ImmutableList<Id> ids = ImmutableList.of(id);
        log.trace("Attempt to read and write {}", id.toString());
        ListenableFuture<Resolved<Content>> resolved = contentResolver.resolveIds(ids);
        Futures.addCallback(resolved, new FutureCallback<Resolved<Content>>() {

            @Override
            public void onSuccess(Resolved<Content> result) {
                for (Content content : result.getResources()) {
                    try {
                        log.trace("writing content " + content);
                        writer.writeContent(content);
                        migrateExplicitEquivalence(content);
                        log.trace("Finished writing content " + content);
                    } catch (MissingResourceException mre) {
                        log.warn("missing {} for {}, re-attempting", mre.getMissingId(), content);
                        readAndWrite(mre.getMissingId());
                        readAndWrite(id, attempt + 1);
                    } catch (WriteException we) {
                        log.error("failed to write " + id + "-" + content, we);
                    }
                }
            }

            @Override
            public void onFailure(Throwable t) {
                log.error("Failed to resolve id" + id);
            }
        });
    }

    private void migrateExplicitEquivalence(Content content) {
        long id = content.getId().longValue();
        try {
            LookupEntry entry = Iterables.getOnlyElement(entryStore.entriesForIds(ImmutableList.of(id)));
            if (entry.explicitEquivalents() == null || entry.explicitEquivalents().isEmpty()) {
                log.warn("Content {} has no explicit equivalents", id);
            } else {
                updateEquivalences(content, entry);
            }
        } catch (WriteException | ExecutionException e) {
            log.warn("Failed to migrate explicit equivalence entries for {} - {}", id, e.toString());
        }
    }

    private void updateEquivalences(Content content, LookupEntry entry) throws WriteException, ExecutionException {
        ImmutableSet<ResourceRef> refs = resolveEquivRefsToResourceRefs(entry.explicitEquivalents());
        ImmutableSet<Publisher> sources = FluentIterable.from(refs).transform(TO_SOURCE).toSet();
        equivalenceGraphStore.updateEquivalences(content.toRef(), refs, sources);
    }

    private ImmutableSet<ResourceRef> resolveEquivRefsToResourceRefs(Set<LookupRef> lookupRefs) throws ExecutionException {
        ImmutableSet<Id> ids = FluentIterable.from(lookupRefs).transform(LookupRef.TO_ID).transform(Id.fromLongValue()).toSet();
        Resolved<Content> contentResolved = Futures.get(contentResolver.resolveIds(ids), ExecutionException.class);
        return contentResolved.getResources().transform(TO_RESOURCE_REF).toSet();
    }
}
