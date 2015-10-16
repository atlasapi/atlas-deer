package org.atlasapi.content;

import static com.google.common.base.Preconditions.checkNotNull;

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
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.base.Optional;
import com.google.common.collect.Collections2;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.metabroadcast.common.collect.OptionalMap;
import com.metabroadcast.common.queue.MessageSender;
import com.metabroadcast.common.queue.MessagingException;
import com.metabroadcast.common.time.Timestamp;

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
        this.equivalentContentUpdatedMessageSender = checkNotNull(equivalentContentUpdatedMessageSender);
    }

    @Override
    public final void updateEquivalences(EquivalenceGraphUpdate update) throws WriteException {
        Set<Id> ids = idsOf(update);
        try {
            lock.lock(ids);
            ImmutableSetMultimap.Builder<EquivalenceGraph, Content> graphsAndContent
                = ImmutableSetMultimap.builder();
            Function<Id, Optional<Content>> toContent = Functions.forMap(resolveIds(ids));
            for (EquivalenceGraph graph : graphsOf(update)) {
                Iterable<Optional<Content>> content = Collections2.transform(graph.getEquivalenceSet(), toContent);
                graphsAndContent.putAll(graph, Optional.presentInstances(content));
            }
            updateEquivalences(graphsAndContent.build(), update);
        } catch (InterruptedException e) {
            throw new WriteException("Updating " + ids, e);
        } finally {
            lock.unlock(ids);
        }
    }

    @Override
    public final void updateContent(Content content) throws WriteException {
        try {
            lock.lock(content.getId());
            ImmutableList<Id> ids = ImmutableList.of(content.getId());

            ListenableFuture<OptionalMap<Id, EquivalenceGraph>> graphs = graphStore.resolveIds(ids);
            Optional<EquivalenceGraph> possibleGraph = get(graphs).get(content.getId());

            EquivalenceGraph graph;
            if (possibleGraph.isPresent()) {
                graph = possibleGraph.get();
                updateInSet(graph, content);

            } else {
                graph = EquivalenceGraph.valueOf(content.toRef());
                updateEquivalences(ImmutableSetMultimap.of(graph, content),
                        EquivalenceGraphUpdate.builder(graph).build());
            }

            sendEquivalentContentChangedMessage(content, graph);
        } catch (MessagingException | InterruptedException e) {
            throw new WriteException("Updating " + content.getId(), e);
        } finally {
            lock.unlock(content.getId());
        }
    }

    protected abstract void updateInSet(EquivalenceGraph graph, Content content);

    protected abstract void updateEquivalences(ImmutableSetMultimap<EquivalenceGraph, Content> graphsAndContent,
            EquivalenceGraphUpdate update);

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
                .addAll(Iterables.concat(Iterables.transform(update.getCreated(),
                        EquivalenceGraph::getEquivalenceSet
                )))
                .addAll(update.getDeleted())
                .build();
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
                )
        );
    }

}
