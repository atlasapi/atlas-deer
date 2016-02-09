package org.atlasapi.system.legacy;

import java.util.Iterator;

import org.atlasapi.media.entity.Content;
import org.atlasapi.media.entity.Identified;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.persistence.content.KnownTypeContentResolver;
import org.atlasapi.persistence.content.ResolvedContent;
import org.atlasapi.persistence.content.listing.ContentListingProgress;
import org.atlasapi.persistence.lookup.entry.LookupEntry;
import org.atlasapi.persistence.lookup.entry.LookupEntryStore;

import com.metabroadcast.common.base.Maybe;

import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;

import static com.google.common.base.Preconditions.checkNotNull;

public class LegacyLookupResolvingContentLister implements LegacyContentLister {

    private final LookupEntryStore lookupEntryStore;
    private final KnownTypeContentResolver contentResolver;

    public LegacyLookupResolvingContentLister(LookupEntryStore lookupEntryStore,
            KnownTypeContentResolver contentResolver) {
        this.lookupEntryStore = checkNotNull(lookupEntryStore);
        this.contentResolver = checkNotNull(contentResolver);
    }

    @Override
    public Iterator<Content> listContent(Iterable<Publisher> publishers,
            ContentListingProgress progress) {
        return iteratorsFor(publishers, progress);
    }

    private Iterator<Content> iteratorsFor(Iterable<Publisher> publishers,
            ContentListingProgress progress) {
        Iterable<LookupEntry> entries = lookupEntryStore.allEntriesForPublishers(
                publishers, progress
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
