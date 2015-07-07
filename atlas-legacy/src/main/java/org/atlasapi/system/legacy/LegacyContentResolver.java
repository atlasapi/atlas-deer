package org.atlasapi.system.legacy;

import java.util.Collection;
import java.util.Map;
import java.util.Map.Entry;

import org.atlasapi.content.Content;
import org.atlasapi.content.ContentResolver;
import org.atlasapi.entity.Alias;
import org.atlasapi.entity.Id;
import org.atlasapi.entity.util.Resolved;
import org.atlasapi.media.channel.ChannelResolver;
import org.atlasapi.media.entity.LookupRef;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.persistence.content.KnownTypeContentResolver;
import org.atlasapi.persistence.content.ResolvedContent;
import org.atlasapi.persistence.lookup.entry.LookupEntry;
import org.atlasapi.persistence.lookup.entry.LookupEntryStore;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.ImmutableSetMultimap.Builder;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.metabroadcast.common.base.MorePredicates;
import com.metabroadcast.common.collect.ImmutableOptionalMap;
import com.metabroadcast.common.collect.OptionalMap;


public class LegacyContentResolver implements ContentResolver {

    private LookupEntryStore lookupStore;
    private KnownTypeContentResolver contentResolver;
    private LegacyContentTransformer transformer;

    public LegacyContentResolver(LookupEntryStore lookupStore, KnownTypeContentResolver contentResolver, LegacySegmentMigrator legacySegmentMigrator,
                                 ChannelResolver channelResolver) {
        this.lookupStore = lookupStore;
        this.contentResolver = contentResolver;
        this.transformer = new LegacyContentTransformer(channelResolver, legacySegmentMigrator);
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

    @Override
    public OptionalMap<Alias, Content> resolveAliases(Iterable<Alias> aliases, Publisher source) {
        ImmutableSet<Alias> uniqueAliases = ImmutableSet.copyOf(aliases);
        Multimap<String, String> aliasMap = index(uniqueAliases);
        Iterable<Iterable<LookupEntry>> entries = Iterables.transform(aliasMap.asMap().entrySet(),
            new Function<Entry<String, Collection<String>>,Iterable<LookupEntry>>(){
                @Override
                public Iterable<LookupEntry> apply(Entry<String, Collection<String>> input) {
                    Optional<String> namspace = Optional.of(input.getKey());
                    return lookupStore.entriesForAliases(namspace, input.getValue());
                }
            });
        Iterable<LookupEntry> allEntries = Iterables.concat(entries);
        Iterable<LookupRef> refs = Iterables.transform(allEntries, LookupEntry.TO_SELF);
        Predicate<LookupRef> filter = MorePredicates.transformingPredicate(LookupRef.TO_SOURCE, Predicates.equalTo(source));
        Iterable<LookupRef> filtered = Iterables.filter(refs, filter);
        ResolvedContent resolved = contentResolver.findByLookupRefs(filtered);
        Iterable<org.atlasapi.media.entity.Content> content = filterContent(resolved);
        Iterable<Content> transformed = transformer.transform(content);
        Map<Alias, Content> aliasToContent = Maps.newHashMap();
        for (Content c : transformed) {
            for (Alias alias : c.getAliases()) {
                aliasToContent.put(alias, c);
            }
        }
        Predicate<Alias> aliasFilter = Predicates.in(uniqueAliases);
        return ImmutableOptionalMap.fromMap(Maps.filterKeys(aliasToContent, aliasFilter));
    }

    private Multimap<String, String> index(Iterable<Alias> aliases) {
        Builder<String,String> index = ImmutableSetMultimap.builder();
        for (Alias alias : aliases) {
            index.put(alias.getNamespace(), alias.getValue());
        }
        return index.build();
    }

}
