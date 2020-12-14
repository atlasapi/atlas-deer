package org.atlasapi.elasticsearch.content;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.metabroadcast.common.query.Selection;
import com.metabroadcast.common.stream.MoreCollectors;
import com.metabroadcast.common.stream.MoreStreams;
import org.atlasapi.content.ContentSearcher;
import org.atlasapi.content.IndexQueryResult;
import org.atlasapi.criteria.AttributeQuery;
import org.atlasapi.entity.Id;
import org.atlasapi.media.entity.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.atlasapi.criteria.attribute.Attributes.Q;

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

            boolean isFuzzyQuery = MoreStreams.stream(query)
                    .anyMatch(attribute ->
                            attribute.getAttribute().externalName().equals(Q.externalName())
                    );

            ImmutableList<Id> dedupedIds = dedupeIds(result, publishers, isFuzzyQuery);
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

        if (delegateLimit > 10000) {
            return Selection.limitedTo(10000);
        } else {
            return Selection.limitedTo(delegateLimit);
        }
    }

    private ImmutableList<Id> dedupeIds(
            DelegateIndexQueryResult result,
            Iterable<Publisher> publishers,
            boolean isFuzzyQuery
    ) {
        LinkedHashMap<Id, DelegateIndexQueryResult.Result> dedupedEntries = new LinkedHashMap<>();

        for (DelegateIndexQueryResult.Result entry : result.getResults()) {
            addToDedupedResults(entry, dedupedEntries, publishers, isFuzzyQuery);
        }

        Stream<Map.Entry<Id, DelegateIndexQueryResult.Result>> dedupedEntriesStream =
                dedupedEntries.entrySet().stream();

        if (isFuzzyQuery) {
            dedupedEntriesStream = dedupedEntriesStream.sorted(
                    Comparator.<Map.Entry<Id, DelegateIndexQueryResult.Result>, Float>comparing(
                            entry -> entry.getValue().getScore()
                    ).reversed()
            );
        }
        return dedupedEntriesStream.map(entry -> entry.getValue().getId())
                .collect(MoreCollectors.toImmutableList());
    }

    private void addToDedupedResults(
            DelegateIndexQueryResult.Result entry,
            LinkedHashMap<Id, DelegateIndexQueryResult.Result> dedupedEntries,
            Iterable<Publisher> publishers,
            boolean isFuzzyQuery
    ) {
        Id canonicalId = entry.getCanonicalId();

        if (!dedupedEntries.containsKey(canonicalId)) {
            dedupedEntries.put(canonicalId, entry);
            return;
        }

        DelegateIndexQueryResult.Result existingEntry = dedupedEntries.get(canonicalId);
        if (hasHigherPrecedence(entry, existingEntry, publishers)) {
            if (isFuzzyQuery) {
                // The order of entries in the map will not matter since we'll sort on score when getting the ids
                if (!Objects.equals(entry.getTitle(), existingEntry.getTitle())) {
                    // Replace the existing entry with the new entry but lower the score a bit.
                    // The tweaking of score is arbitrary here; originally no special logic was used for fuzzy queries
                    // and the result would be positioned as if it had the lower of the two scores.
                    // The original logic can be thought of using the lower score because it would be the one whose
                    // title is returned in the merged results.
                    // However since we boost on other factors than title we want to preserve some of that score
                    // and opt for taking an average of the two for now.
                    dedupedEntries.put(
                            canonicalId,
                            DelegateIndexQueryResult.Result.of(
                                    entry.getId(),
                                    (entry.getScore() + existingEntry.getScore()) / 2f,
                                    entry.getCanonicalId(),
                                    entry.getPublisher(),
                                    entry.getTitle()
                            )
                    );
                } else {
                    // Replace the existing entry with the new entry but keeping the original score
                    dedupedEntries.put(
                            canonicalId,
                            DelegateIndexQueryResult.Result.of(
                                    entry.getId(),
                                    existingEntry.getScore(),
                                    entry.getCanonicalId(),
                                    entry.getPublisher(),
                                    entry.getTitle()
                            )
                    );
                }
            } else {
                // Remove before put to ensure we update the ordering for this canonical ID
                // to the position of this entry
                dedupedEntries.remove(canonicalId);
                dedupedEntries.put(
                        canonicalId,
                        entry
                );
            }
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
