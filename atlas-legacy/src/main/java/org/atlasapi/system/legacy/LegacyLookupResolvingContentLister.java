package org.atlasapi.system.legacy;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Iterator;

import org.atlasapi.media.entity.Content;
import org.atlasapi.media.entity.Identified;
import org.atlasapi.persistence.content.KnownTypeContentResolver;
import org.atlasapi.persistence.content.ResolvedContent;
import org.atlasapi.persistence.content.listing.ContentListingCriteria;
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

    @Override
    public Iterator<Content> listContent(ContentListingCriteria criteria) {
        if (criteria.getPublishers().isEmpty()) {
            return Iterators.emptyIterator();
        }

        return iteratorsFor(criteria);
    }

    private Iterator<Content> iteratorsFor(ContentListingCriteria criteria) {
        Iterable<LookupEntry> entries = lookupEntryStore.allEntriesForPublishers(criteria);

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
