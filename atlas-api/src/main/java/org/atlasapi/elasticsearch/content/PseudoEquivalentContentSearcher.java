package org.atlasapi.elasticsearch.content;

import java.util.LinkedHashMap;
import java.util.Set;
import java.util.stream.StreamSupport;

import org.atlasapi.content.ContentSearcher;
import org.atlasapi.content.IndexQueryResult;
import org.atlasapi.criteria.AttributeQuery;
import org.atlasapi.entity.Id;
import org.atlasapi.media.entity.Publisher;

import com.metabroadcast.common.query.Selection;
import com.metabroadcast.common.stream.MoreCollectors;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkNotNull;

public class PseudoEquivalentContentSearcher implements ContentSearcher {

    private static final Logger LOG = LoggerFactory.getLogger(PseudoEquivalentContentSearcher.class);

    private final EsUnequivalentContentSearcher delegate;

    private PseudoEquivalentContentSearcher(EsUnequivalentContentSearcher delegate) {
        this.delegate = checkNotNull(delegate);
    }

    public static PseudoEquivalentContentSearcher create(EsUnequivalentContentSearcher delegate) {
        return new PseudoEquivalentContentSearcher(delegate);
    }

    @Override
    public ListenableFuture<IndexQueryResult> query(
            Iterable<AttributeQuery<?>> query,
            Iterable<Publisher> publishers,
            Selection selection
    ) {
        try {

            Selection selectionForDelegate = getSelectionForDelegate(publishers, selection);

            DelegateIndexQueryResult result = Futures.get(
                    delegate.delegateQuery(query, publishers, selectionForDelegate),
                    Exception.class
            );

            ImmutableList<Id> dedupedIds = dedupeIds(result, publishers);
            ImmutableList<Id> paginatedIds = paginateIds(dedupedIds, selection);

            return Futures.immediateFuture(
                    IndexQueryResult.withIds(paginatedIds, result.getTotalCount())
            );

        } catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }

    private Selection getSelectionForDelegate(Iterable<Publisher> publishers, Selection selection) {
        int numberOfSources = Iterables.size(publishers);
        int delegateLimit = selection.limitOrDefaultValue(100) * numberOfSources;

        if (selection.hasNonZeroOffset()) {
            delegateLimit += selection.getOffset() * numberOfSources;
        }

        return Selection.limitedTo(delegateLimit);
    }

    private ImmutableList<Id> dedupeIds(DelegateIndexQueryResult result,
            Iterable<Publisher> publishers) {
        LinkedHashMap<Id, DelegateIndexQueryResult.Result> dedupedEntries = new LinkedHashMap<>();

        for (DelegateIndexQueryResult.Result entry : result.getResults()) {
            addToDedupedResults(entry, dedupedEntries, publishers);
        }

        return dedupedEntries.entrySet().stream()
                .map(entry -> entry.getValue().getId())
                .collect(MoreCollectors.toImmutableList());
    }

    private void addToDedupedResults(DelegateIndexQueryResult.Result entry,
            LinkedHashMap<Id, DelegateIndexQueryResult.Result> dedupedEntries,
            Iterable<Publisher> publishers) {
        Id canonicalId = entry.getCanonicalId();

        if (!dedupedEntries.containsKey(canonicalId)) {
            dedupedEntries.put(canonicalId, entry);
            return;
        }

        DelegateIndexQueryResult.Result existingEntry = dedupedEntries.get(canonicalId);
        if (hasHigherPrecedence(entry, existingEntry, publishers)) {
            // Remove before put to ensure we update the ordering for this canonical ID
            // to the position of this entry
            dedupedEntries.remove(canonicalId);
            dedupedEntries.put(canonicalId, entry);
        }
    }

    private boolean hasHigherPrecedence(DelegateIndexQueryResult.Result currentEntry,
            DelegateIndexQueryResult.Result existingEntry, Iterable<Publisher> publishers) {
        return hasHigherPrecedence(
                currentEntry.getPublisher(), existingEntry.getPublisher(), publishers
        );
    }

    private boolean hasHigherPrecedence(Publisher currentPublisher, Publisher existingPublisher,
            Iterable<Publisher> publisherPrecedence) {
        for (Publisher publisher : publisherPrecedence) {
            if (publisher == existingPublisher) {
                return false;
            }
            if (publisher == currentPublisher) {
                return true;
            }
        }

        // This should never happen as the delegate uses the same publishers for the query
        LOG.error("Delegate content index returned publisher {} that was not asked for."
                + " Requested publishers: {}", currentPublisher, publisherPrecedence);
        return false;
    }

    private ImmutableList<Id> paginateIds(Iterable<Id> ids, Selection selection) {
        return StreamSupport.stream(ids.spliterator(), false)
                .skip(selection.hasNonZeroOffset() ? selection.getOffset() : 0)
                .limit(selection.limitOrDefaultValue(100))
                .collect(MoreCollectors.toImmutableList());
    }
}
