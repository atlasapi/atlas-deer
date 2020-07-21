package org.atlasapi.system.bootstrap.workers;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.Futures;
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

import java.util.Set;
import java.util.concurrent.ExecutionException;

import static com.google.common.base.Preconditions.checkNotNull;

public class DirectAndExplicitEquivalenceMigrator {

    private static final Function<Content, ResourceRef> TO_RESOURCE_REF = Content::toRef;

    private final Logger log = LoggerFactory.getLogger(DirectAndExplicitEquivalenceMigrator.class);
    private final ContentResolver legacyResolver;
    private final LookupEntryStore legacyEquivalenceStore;
    private final EquivalenceGraphStore graphStore;

    private DirectAndExplicitEquivalenceMigrator(
            ContentResolver legacyResolver,
            LookupEntryStore legacyEquivalenceStore,
            EquivalenceGraphStore graphStore
    ) {
        this.legacyResolver = checkNotNull(legacyResolver);
        this.legacyEquivalenceStore = checkNotNull(legacyEquivalenceStore);
        this.graphStore = checkNotNull(graphStore);
    }

    public static DirectAndExplicitEquivalenceMigrator create(
            ContentResolver legacyResolver,
            LookupEntryStore legacyEquivalenceStore,
            EquivalenceGraphStore graphStore
    ) {
        return new DirectAndExplicitEquivalenceMigrator(
                legacyResolver,
                legacyEquivalenceStore,
                graphStore
        );
    }

    public Optional<EquivalenceGraphUpdate> migrateEquivalence(ResourceRef ref) {
        try {
            Id contentId = ref.getId();
            LookupEntry legacyLookupEntry = resolveLegacyEquivalents(contentId);
            Set<LookupRef> legacyEquivRefs = legacyLookupEntry.getOutgoing();
            if (!legacyEquivRefs.isEmpty()) {
                log.debug("Resolved {} legacy equiv refs for {}",
                        legacyEquivRefs.size(), contentId
                );
                log.trace("Legacy equiv refs for {} resolved as {}", contentId, legacyEquivRefs);
                Set<ResourceRef> equivRefs = resolveLegacyContent(legacyEquivRefs);
                log.debug("Dereferenced {} of {} explicit equivalents for {}",
                        equivRefs.size(), legacyEquivRefs.size(), contentId
                );
                Optional<EquivalenceGraphUpdate> graphUpdate = updateGraphStore(ref, equivRefs);
                log.debug("Updated graph store for {}? {}", contentId, graphUpdate.isPresent());
                if (graphUpdate.isPresent()) {
                    log.trace("Graph update for ref {} is {}", contentId, graphUpdate.get());
                }
                return graphUpdate;
            } else {
                log.warn("Content {} has no explicit equivalents", contentId);
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
        Resolved<Content> contentResolved = Futures.getChecked(
                legacyResolver.resolveIds(ids),
                ExecutionException.class
        );
        return contentResolved.getResources().transform(TO_RESOURCE_REF).toSet();
    }

    private Optional<EquivalenceGraphUpdate> updateGraphStore(ResourceRef ref,
            Set<ResourceRef> refs)
            throws WriteException {
        return graphStore.updateEquivalences(ref, refs, Publisher.all());
    }

    private LookupEntry resolveLegacyEquivalents(Id id) {
        return Iterables.getOnlyElement(legacyEquivalenceStore.entriesForIds(ImmutableList.of(id.longValue())));
    }
}
