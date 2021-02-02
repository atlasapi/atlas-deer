package org.atlasapi.query.v4.search;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.metabroadcast.common.query.Selection;
import com.metabroadcast.common.stream.MoreCollectors;
import com.metabroadcast.common.stream.MoreStreams;
import com.metabroadcast.sherlock.client.response.ContentResult;
import com.metabroadcast.sherlock.client.response.ContentSearchQueryResponse;
import com.metabroadcast.sherlock.client.search.SearchQuery;
import com.metabroadcast.sherlock.client.search.SherlockSearcher;
import org.atlasapi.content.IndexQueryResult;
import org.atlasapi.entity.Id;
import org.atlasapi.media.entity.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class PseudoEsEquivalentContentSearcher {

    private static final Logger log = LoggerFactory.getLogger(PseudoEsEquivalentContentSearcher.class);

    private final SherlockSearcher sherlockSearcher;

    private PseudoEsEquivalentContentSearcher(SherlockSearcher sherlockSearcher) {
        this.sherlockSearcher = sherlockSearcher;
    }

    public static PseudoEsEquivalentContentSearcher create(SherlockSearcher sherlockSearcher) {
        return new PseudoEsEquivalentContentSearcher(sherlockSearcher);
    }

    public ListenableFuture<IndexQueryResult> searchForContent(
            SearchQuery.Builder searchQueryBuilder,
            Iterable<Publisher> precedentOrderedPublishers,
            Selection selection
    ) {
        List<String> precedentOrderedSources = MoreStreams.stream(precedentOrderedPublishers)
                .map(Publisher::key)
                .collect(MoreCollectors.toImmutableList());


        Selection selectionForDelegate = getSelectionForDelegate(
                precedentOrderedSources,
                selection
        );

        SearchQuery searchQuery = searchQueryBuilder
                .withLimit(selectionForDelegate.getLimit())
                .withOffset(selectionForDelegate.getOffset())
                .build();

        boolean isFuzzyQuery = !searchQuery.getSearchers().isEmpty();

        ListenableFuture<ContentSearchQueryResponse> responseFuture = sherlockSearcher.searchForContent(searchQuery);

        return Futures.transform(
                responseFuture,
                (Function<ContentSearchQueryResponse, IndexQueryResult>) response -> {
                    ImmutableList<Id> dedupedIds = dedupeIds(response, precedentOrderedSources, isFuzzyQuery);
                    ImmutableList<Id> paginatedIds = paginateIds(dedupedIds, selection);
                    return IndexQueryResult.withIds(paginatedIds, response.getTotalResults());
                }
        );
    }

    private Selection getSelectionForDelegate(
            List<String> sources,
            Selection selection
    ) {
        int numberOfSources = sources.size();
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
            ContentSearchQueryResponse response,
            Iterable<String> sources,
            boolean isFuzzyQuery
    ) {
        LinkedHashMap<Id, ContentResult> dedupedEntries = new LinkedHashMap<>();

        for (ContentResult result : response.getResults()) {
            addToDedupedResults(result, dedupedEntries, sources, isFuzzyQuery);
        }

        Stream<Map.Entry<Id, ContentResult>> dedupedEntriesStream = dedupedEntries.entrySet().stream();

        if (isFuzzyQuery) {
            dedupedEntriesStream = dedupedEntriesStream.sorted(
                    Comparator.<Map.Entry<Id, ContentResult>, Float>comparing(
                            entry -> entry.getValue().getScore()
                    ).reversed()
            );
        }
        return dedupedEntriesStream.map(entry -> entry.getValue().getId())
                .map(Id::valueOf)
                .collect(MoreCollectors.toImmutableList());
    }

    private void addToDedupedResults(
            ContentResult entry,
            LinkedHashMap<Id, ContentResult> dedupedEntries,
            Iterable<String> sources,
            boolean isFuzzyQuery
    ) {
        Id canonicalId = Id.valueOf(entry.getCanonicalId());

        if (!dedupedEntries.containsKey(canonicalId)) {
            dedupedEntries.put(canonicalId, entry);
            return;
        }

        ContentResult existingEntry = dedupedEntries.get(canonicalId);
        if (hasHigherPrecedence(entry, existingEntry, sources)) {
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
                            new ContentResult(
                                    entry.getId(),
                                    (entry.getScore() + existingEntry.getScore()) / 2f,
                                    entry.getCanonicalId(),
                                    entry.getSource(),
                                    entry.getTitle()
                            )
                    );
                } else {
                    // Replace the existing entry with the new entry but keeping the original score
                    dedupedEntries.put(
                            canonicalId,
                            new ContentResult(
                                    entry.getId(),
                                    existingEntry.getScore(),
                                    entry.getCanonicalId(),
                                    entry.getSource(),
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

    private boolean hasHigherPrecedence(
            ContentResult currentEntry,
            ContentResult existingEntry,
            Iterable<String> sources
    ) {
        return hasHigherPrecedence(
                currentEntry.getSource(),
                existingEntry.getSource(),
                sources
        );
    }

    private boolean hasHigherPrecedence(
            String currentSource,
            String existingSource,
            Iterable<String> sourcePrecedence
    ) {
        for (String source : sourcePrecedence) {
            if (source.equals(existingSource)) {
                return false;
            }
            if (source.equals(currentSource)) {
                return true;
            }
        }

        // This should never happen as the delegate uses the same publishers for the query
        log.error(
                "Delegate content index returned source {} that was not asked for."
                        + " Requested sources: {}",
                currentSource,
                sourcePrecedence
        );
        return false;
    }

    private ImmutableList<Id> paginateIds(Iterable<Id> ids, Selection selection) {
        return StreamSupport.stream(ids.spliterator(), false)
                .skip(selection.hasNonZeroOffset() ? selection.getOffset() : 0)
                .limit(selection.limitOrDefaultValue(100))
                .collect(MoreCollectors.toImmutableList());
    }

}
