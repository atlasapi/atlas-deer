package org.atlasapi.system.bootstrap.workers;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Set;
import java.util.concurrent.ExecutionException;

import com.google.common.base.Throwables;
import com.google.common.collect.Sets;
import org.atlasapi.content.Content;
import org.atlasapi.content.ContentResolver;
import org.atlasapi.entity.Id;
import org.atlasapi.entity.ResourceRef;
import org.atlasapi.entity.util.Resolved;
import org.atlasapi.entity.util.WriteException;
import org.atlasapi.equivalence.EquivalenceGraphStore;
import org.atlasapi.equivalence.EquivalenceGraphUpdate;
import org.atlasapi.media.entity.LookupRef;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.persistence.lookup.entry.LookupEntry;
import org.atlasapi.persistence.lookup.entry.LookupEntryStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.Futures;

public class DirectAndExplicitEquivalenceMigrator {

    private static final Function<Content, ResourceRef> TO_RESOURCE_REF = new Function<Content, ResourceRef>() {

        @Override
        public ResourceRef apply(Content input) {
            return input.toRef();
        }
    };

    private static final Function<ResourceRef, Publisher> TO_SOURCE = new Function<ResourceRef, Publisher>() {

        @Override
        public Publisher apply(ResourceRef input) {
            return input.getSource();
        }
    };

    private final Logger log = LoggerFactory.getLogger(DirectAndExplicitEquivalenceMigrator.class);
    private final ContentResolver legacyResolver;
    private final LookupEntryStore legacyEquivalenceStore;
    private final EquivalenceGraphStore graphStore;

    public DirectAndExplicitEquivalenceMigrator(ContentResolver legacyResolver,
                                                LookupEntryStore legacyEquivalenceStore, EquivalenceGraphStore graphStore) {
        this.legacyResolver = checkNotNull(legacyResolver);
        this.legacyEquivalenceStore = checkNotNull(legacyEquivalenceStore);
        this.graphStore = checkNotNull(graphStore);
    }

    public Optional<EquivalenceGraphUpdate> migrateEquivalence(Content content) {
        try {
            LookupEntry legacyLookupEntry = resolveLegacyEquivalents(content);
            Set<LookupRef> legacyEquivRefs = Sets.union(legacyLookupEntry.explicitEquivalents(), legacyLookupEntry.directEquivalents());
            if (!legacyEquivRefs.isEmpty()) {
                log.trace("Resolved {} legacy explicit equiv refs for {}",
                        legacyEquivRefs.size(), content.getId());
                Set<ResourceRef> equivRefs = resolveLegacyContent(legacyEquivRefs);
                log.trace("Dereferenced {} of {} explicit equivalents for {}",
                        equivRefs.size(), legacyEquivRefs.size(), content.getId());
                Optional<EquivalenceGraphUpdate> graphUpdate = updateGraphStore(
                        content,
                        equivRefs);
                log.trace("Updated graph store for {}? {}", content.getId(), graphUpdate.isPresent());
                return graphUpdate;
            } else {
                log.warn("Content {} has no explicit equivalents", content.getId());
                return Optional.absent();
            }
        } catch (WriteException | ExecutionException e) {
            throw Throwables.propagate(e);
        }
    }

    private Set<ResourceRef> resolveLegacyContent(Set<LookupRef> legacyEquivRefs)
            throws ExecutionException {
        ImmutableSet<Id> ids = FluentIterable.from(legacyEquivRefs)
                .transform(LookupRef.TO_ID)
                .transform(Id.fromLongValue())
                .toSet();
        Resolved<Content> contentResolved = Futures.get(legacyResolver.resolveIds(ids), ExecutionException.class);
        return contentResolved.getResources().transform(TO_RESOURCE_REF).toSet();
    }

    private Optional<EquivalenceGraphUpdate> updateGraphStore(Content content, Set<ResourceRef> refs)
            throws WriteException {
        ImmutableSet<Publisher> sources = FluentIterable.from(refs).transform(TO_SOURCE).toSet();
        return graphStore.updateEquivalences(content.toRef(), refs, sources);
    }

    private LookupEntry resolveLegacyEquivalents(Content content) {
        long id = content.getId().longValue();
        return Iterables.getOnlyElement(legacyEquivalenceStore.entriesForIds(ImmutableList.of(id)));
    }

}
