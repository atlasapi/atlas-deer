package org.atlasapi.content;

import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.atlasapi.entity.Id;
import org.atlasapi.entity.util.WriteException;
import org.atlasapi.equivalence.EquivalenceGraph;
import org.atlasapi.equivalence.EquivalenceGraphStore;
import org.atlasapi.equivalence.EquivalenceGraphUpdate;
import org.atlasapi.messaging.EquivalentContentUpdatedMessage;
import org.atlasapi.util.GroupLock;
import org.atlasapi.util.ImmutableCollectors;

import com.metabroadcast.common.collect.OptionalMap;
import com.metabroadcast.common.queue.MessageSender;
import com.metabroadcast.common.queue.MessagingException;
import com.metabroadcast.common.time.Timestamp;

import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.base.Optional;
import com.google.common.collect.Collections2;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.Iterables;
import com.google.common.primitives.Longs;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkNotNull;

public abstract class AbstractEquivalentContentStore implements EquivalentContentStore {

    private static final Logger LOG = LoggerFactory.getLogger(AbstractEquivalentContentStore.class);

    private final ContentResolver contentResolver;

    private final EquivalenceGraphStore graphStore;
    private final MessageSender<EquivalentContentUpdatedMessage> equivalentContentUpdatedMessageSender;
    private static final GroupLock<Id> lock = GroupLock.natural();

    public AbstractEquivalentContentStore(
            ContentResolver contentResolver,
            EquivalenceGraphStore graphStore,
            MessageSender<EquivalentContentUpdatedMessage> equivalentContentUpdatedMessageSender
    ) {
        this.contentResolver = checkNotNull(contentResolver);
        this.graphStore = checkNotNull(graphStore);
        this.equivalentContentUpdatedMessageSender = checkNotNull(
                equivalentContentUpdatedMessageSender);
    }

    @Override
    public final void updateEquivalences(EquivalenceGraphUpdate update) throws WriteException {
        Set<Id> ids = idsOf(update);
        ImmutableSet<Id> staleContentIds = ImmutableSet.of();

        try {
            lock.lock(ids);

            ImmutableSetMultimap.Builder<EquivalenceGraph, Content> graphsAndContentBuilder
                    = ImmutableSetMultimap.builder();
            Function<Id, Optional<Content>> toContent = Functions.forMap(resolveIds(ids));

            for (EquivalenceGraph graph : graphsOf(update)) {
                Iterable<Optional<Content>> content =
                        Collections2.transform(graph.getEquivalenceSet(), toContent);
                graphsAndContentBuilder.putAll(graph, Optional.presentInstances(content));
            }

            ImmutableSetMultimap<EquivalenceGraph, Content> graphsAndContent =
                    graphsAndContentBuilder.build();

            // This has to run before we process the update so we can still resolve the set(s)
            // that are going to be deleted
            staleContentIds = getStaleContent(
                    update.getDeleted(),
                    graphsAndContent
            );

            update(graphsAndContent, update);
        } catch (InterruptedException e) {
            throw new WriteException("Updating " + ids, e);
        } finally {
            lock.unlock(ids);
        }

        // We are updating stale content after we have already released our held IDs because
        // the ID(s) of the deleted graph(s) on which we hold a lock could be among the stale
        // content IDs and the lock is not reentrant
        updateStaleContent(staleContentIds);
    }

    @Override
    public final void updateContent(Id contentId) throws WriteException {
        try {
            lock.lock(contentId);

            Content content = resolveId(contentId);

            ImmutableList<Id> ids = ImmutableList.of(contentId);

            ListenableFuture<OptionalMap<Id, EquivalenceGraph>> graphs = graphStore.resolveIds(ids);
            Optional<EquivalenceGraph> possibleGraph = get(graphs).get(content.getId());

            EquivalenceGraph graph;
            if (possibleGraph.isPresent()) {
                graph = possibleGraph.get();
                update(graph, content);
            } else {
                graph = EquivalenceGraph.valueOf(content.toRef());
                update(graph, content);
            }

            sendEquivalentContentChangedMessage(content, graph);
        } catch (MessagingException | InterruptedException e) {
            throw new WriteException("Updating " + contentId, e);
        } finally {
            lock.unlock(contentId);
        }
    }

    protected abstract void update(EquivalenceGraph graph, Content content);

    protected abstract void update(ImmutableSetMultimap<EquivalenceGraph, Content> graphsAndContent,
            EquivalenceGraphUpdate update);

    protected abstract ListenableFuture<Set<Content>> resolveEquivalentSetIncludingStaleContent(
            Long equivalentSetId);

    protected ContentResolver getContentResolver() {
        return contentResolver;
    }

    private Iterable<EquivalenceGraph> graphsOf(EquivalenceGraphUpdate update) {
        return ImmutableSet.<EquivalenceGraph>builder()
                .add(update.getUpdated())
                .addAll(update.getCreated())
                .build();
    }

    private ImmutableSet<Id> idsOf(EquivalenceGraphUpdate update) {
        return ImmutableSet.<Id>builder()
                .addAll(update.getUpdated().getEquivalenceSet())
                .addAll(Iterables.concat(Iterables.transform(
                        update.getCreated(),
                        EquivalenceGraph::getEquivalenceSet
                )))
                .addAll(update.getDeleted())
                .build();
    }

    private Content resolveId(Id contentId) throws WriteException {
        Optional<Content> contentOptional = resolveIds(ImmutableList.of(contentId)).get(contentId);

        if (contentOptional.isPresent()) {
            return contentOptional.get();
        }
        throw new WriteException("Failed to resolve content " + contentId);
    }

    private OptionalMap<Id, Content> resolveIds(Iterable<Id> ids) throws WriteException {
        return get(contentResolver.resolveIds(ids)).toMap();
    }

    private <T> T get(ListenableFuture<T> future) throws WriteException {
        return Futures.get(future, 1, TimeUnit.MINUTES, WriteException.class);
    }

    private void sendEquivalentContentChangedMessage(Content content, EquivalenceGraph graph)
            throws MessagingException {
        equivalentContentUpdatedMessageSender.sendMessage(
                new EquivalentContentUpdatedMessage(
                        UUID.randomUUID().toString(),
                        Timestamp.of(DateTime.now(DateTimeZone.UTC)),
                        graph.getId().longValue(),
                        content.toRef()
                ),
                Longs.toByteArray(graph.getId().longValue())
        );
    }

    // This will resolve all content in the graphs that are about to be deleted and then check
    // if that content appears in the updated or created graphs. If not it could be stale content
    // so we are forcing an update
    private ImmutableSet<Id> getStaleContent(ImmutableSet<Id> deletedGraphIds,
            ImmutableSetMultimap<EquivalenceGraph, Content> graphsAndContent) {
        ImmutableSet<Id> idsOfContentToBeUpdated = graphsAndContent.values().stream()
                .map(Content::getId)
                .collect(ImmutableCollectors.toSet());

        return deletedGraphIds.stream()
                .flatMap(graphId -> getStaleContent(graphId, idsOfContentToBeUpdated).stream())
                .collect(ImmutableCollectors.toSet());
    }

    private ImmutableSet<Id> getStaleContent(Id deletedGraphId,
            ImmutableSet<Id> contentIdsToBeUpdated) {
        try {
            return get(resolveEquivalentSetIncludingStaleContent(deletedGraphId.longValue()))
                    .stream()
                    .map(Content::getId)
                    .filter(id -> !contentIdsToBeUpdated.contains(id))
                    .collect(ImmutableCollectors.toSet());

        } catch (WriteException e) {
            LOG.warn("Failed to resolve equivalent set {}", deletedGraphId, e);
        }
        return ImmutableSet.of();
    }

    private void updateStaleContent(ImmutableSet<Id> staleContentIds) {
        staleContentIds.stream()
                .forEach(contentId -> {
                    try {
                        updateContent(contentId);
                    } catch (WriteException e) {
                        LOG.warn("Failed to update stale content {}", contentId, e);
                    }
                });
    }
}
