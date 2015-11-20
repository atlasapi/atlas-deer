package org.atlasapi.system.legacy;

import static com.google.gson.internal.$Gson$Preconditions.checkNotNull;

import java.util.Iterator;
import java.util.List;

import org.atlasapi.media.entity.Content;
import org.atlasapi.media.entity.Identified;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.persistence.content.KnownTypeContentResolver;
import org.atlasapi.persistence.content.ResolvedContent;
import org.atlasapi.persistence.content.listing.ContentListingCriteria;
import org.atlasapi.persistence.content.listing.ContentListingProgress;
import org.atlasapi.persistence.lookup.entry.LookupEntry;
import org.atlasapi.persistence.lookup.entry.LookupEntryStore;

import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;
import com.metabroadcast.common.base.Maybe;

public class LegacyLookupResolvingContentLister implements LegacyContentLister {

    private final LookupEntryStore lookupEntryStore;
    private final KnownTypeContentResolver contentResolver;

    public LegacyLookupResolvingContentLister(LookupEntryStore lookupEntryStore,
            KnownTypeContentResolver contentResolver) {
        this.lookupEntryStore = checkNotNull(lookupEntryStore);
        this.contentResolver = checkNotNull(contentResolver);
    }

    /**
     * This implementation ignores content categories in ContentListingCriteria and only uses
     * Publishers and ContentListingProgress
     */
    @Override
    public Iterator<Content> listContent(ContentListingCriteria criteria) {
        if (criteria.getPublishers().isEmpty()) {
            return Iterators.emptyIterator();
        }

        return iteratorsFor(criteria.getPublishers(), criteria.getProgress());
    }

    private Iterator<Content> iteratorsFor(List<Publisher> publishers,
            ContentListingProgress progress) {
        Iterable<LookupEntry> entries = lookupEntryStore.entriesForPublishers(
                publishers, progress, false
        );

        return FluentIterable.from(entries)
                .transform(this::resolveLookup)
                .filter(Maybe::hasValue)
                .transform(Maybe::requireValue)
                .filter(Content.class)
                .iterator();
    }

    private Maybe<Identified> resolveLookup(LookupEntry lookupEntry) {
        ResolvedContent resolvedContent = contentResolver.findByLookupRefs(
                ImmutableList.of(lookupEntry.lookupRef())
        );
        return resolvedContent.get(lookupEntry.uri());
    }
}
