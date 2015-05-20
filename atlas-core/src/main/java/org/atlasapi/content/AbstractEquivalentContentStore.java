package org.atlasapi.content;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.atlasapi.entity.Id;
import org.atlasapi.entity.ResourceRef;
import org.atlasapi.entity.util.WriteException;
import org.atlasapi.equivalence.EquivalenceGraph;
import org.atlasapi.equivalence.EquivalenceGraphStore;
import org.atlasapi.equivalence.EquivalenceGraphUpdate;
import org.atlasapi.util.GroupLock;
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

public abstract class AbstractEquivalentContentStore implements EquivalentContentStore {

    private static final Logger log = LoggerFactory.getLogger(AbstractEquivalentContentStore.class);
    private final ContentResolver contentResolver;
    private final EquivalenceGraphStore graphStore;

    private static final GroupLock<Id> lock = GroupLock.natural();

    public AbstractEquivalentContentStore(ContentResolver contentResolver, EquivalenceGraphStore graphStore) {
        this.contentResolver = checkNotNull(contentResolver);
        this.graphStore = checkNotNull(graphStore);
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
                new Function<EquivalenceGraph, Set<Id>>() {
                    @Override
                    public Set<Id> apply(EquivalenceGraph input) {
                        return input.getEquivalenceSet();
                    }
                }
            )))
            .addAll(update.getDeleted())
            .build();
    }

    protected abstract void updateEquivalences(ImmutableSetMultimap<EquivalenceGraph, Content> graphsAndContent, 
            EquivalenceGraphUpdate update);

    private OptionalMap<Id, Content> resolveIds(Iterable<Id> ids) throws WriteException {
        return get(contentResolver.resolveIds(ids)).toMap();
    }

    private <T> T get(ListenableFuture<T> future) throws WriteException {
        return Futures.get(future, 1, TimeUnit.MINUTES, WriteException.class);
    }

    @Override
    public final void updateContent(ResourceRef ref) throws WriteException {
        try {
            lock.lock(ref.getId());
            ImmutableList<Id> ids = ImmutableList.of(ref.getId());
            OptionalMap<Id, Content> resolvedContent = resolveIds(ids);
            
            Optional<Content> possibleContent = resolvedContent.get(ref.getId());
            if (!possibleContent.isPresent()) {
                throw new WriteException("update failed. content not found for id " + ref.getId());
            }
            Content content = possibleContent.get();
            
            ListenableFuture<OptionalMap<Id, EquivalenceGraph>> graphs = graphStore.resolveIds(ids);
            Optional<EquivalenceGraph> possibleGraph = get(graphs).get(ref.getId());
            
            if (possibleGraph.isPresent()) {
                updateInSet(possibleGraph.get(), content);
            } else {
                EquivalenceGraph graph = EquivalenceGraph.valueOf(ref);
                updateEquivalences(ImmutableSetMultimap.of(graph, content), EquivalenceGraphUpdate.builder(graph).build());
            }
        } catch (InterruptedException e) {
            throw new WriteException("Updating " + ref.getId(), e);
        } finally {
            lock.unlock(ref.getId());
        }
    }

    protected abstract void updateInSet(EquivalenceGraph graph, Content content);

}
