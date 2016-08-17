package org.atlasapi.system.legacy;

import org.atlasapi.content.Content;
import org.atlasapi.content.ContentResolver;
import org.atlasapi.entity.Alias;
import org.atlasapi.entity.Id;
import org.atlasapi.entity.util.Resolved;
import org.atlasapi.media.entity.LookupRef;
import org.atlasapi.persistence.content.KnownTypeContentResolver;
import org.atlasapi.persistence.content.ResolvedContent;
import org.atlasapi.persistence.lookup.entry.LookupEntry;
import org.atlasapi.persistence.lookup.entry.LookupEntryStore;

import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.ImmutableSetMultimap.Builder;
import com.google.common.collect.Iterables;
import com.google.common.collect.Multimap;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import static com.google.common.base.Preconditions.checkNotNull;

public class LegacyContentResolver implements ContentResolver {

    private LookupEntryStore lookupStore;
    private KnownTypeContentResolver contentResolver;
    private LegacyContentTransformer transformer;

    public LegacyContentResolver(LookupEntryStore lookupStore,
            KnownTypeContentResolver contentResolver,
            LegacyContentTransformer legacyContentTransformer) {
        this.lookupStore = lookupStore;
        this.contentResolver = contentResolver;
        this.transformer = checkNotNull(legacyContentTransformer);
    }

    protected LegacyContentResolver() {

    }

    @Override
    public ListenableFuture<Resolved<Content>> resolveIds(Iterable<Id> ids) {
        Iterable<Long> lids = Iterables.transform(ids, Id.toLongValue());
        Iterable<LookupEntry> entries = lookupStore.entriesForIds(lids);
        Iterable<LookupRef> refs = Iterables.transform(entries, LookupEntry.TO_SELF);
        ResolvedContent resolved = contentResolver.findByLookupRefs(refs);
        Iterable<org.atlasapi.media.entity.Content> content = filterContent(resolved);
        Iterable<Content> transformed = transformer.transform(content);
        return Futures.immediateFuture(Resolved.valueOf(transformed));
    }

    private Iterable<org.atlasapi.media.entity.Content> filterContent(ResolvedContent resolved) {
        Class<org.atlasapi.media.entity.Content> cls = org.atlasapi.media.entity.Content.class;
        return Iterables.filter(resolved.getAllResolvedResults(), cls);
    }

    private Multimap<String, String> index(Iterable<Alias> aliases) {
        Builder<String, String> index = ImmutableSetMultimap.builder();
        for (Alias alias : aliases) {
            index.put(alias.getNamespace(), alias.getValue());
        }
        return index.build();
    }

}
