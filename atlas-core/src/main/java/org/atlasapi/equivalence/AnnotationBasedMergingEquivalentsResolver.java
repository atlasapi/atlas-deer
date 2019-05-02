package org.atlasapi.equivalence;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import com.metabroadcast.applications.client.model.internal.Application;
import org.atlasapi.annotation.Annotation;
import org.atlasapi.entity.Id;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkNotNull;

public class AnnotationBasedMergingEquivalentsResolver<E extends Equivalable<E>>
        implements MergingEquivalentsResolver<E> {

    private static final Logger log = LoggerFactory.getLogger(AnnotationBasedMergingEquivalentsResolver.class);

    private final EquivalentsResolver<E> resolver;
    private final ApplicationEquivalentsMerger<E> merger;

    public AnnotationBasedMergingEquivalentsResolver(EquivalentsResolver<E> resolver,
            ApplicationEquivalentsMerger<E> merger) {
        this.resolver = checkNotNull(resolver);
        this.merger = checkNotNull(merger);
    }

    @Override
    public ListenableFuture<ResolvedEquivalents<E>> resolveIds(Iterable<Id> ids,
            Application application, Set<Annotation> activeAnnotations) {

        if (activeAnnotations.contains(Annotation.NON_MERGED)) {
            return resolver.resolveIdsWithoutEquivalence(
                    ids,
                    application.getConfiguration().getEnabledReadSources(),
                    activeAnnotations
            );
        } else {
            ListenableFuture<ResolvedEquivalents<E>> unmerged
                    = resolver.resolveIds(ids, application.getConfiguration().getEnabledReadSources(), activeAnnotations);
            try {
                if(unmerged.get()==null){
                    log.error("Failed to resolve unmerged equivalences for {}",
                            StreamSupport.stream(ids.spliterator(), false)
                                    .map(Id::toString)
                                    .collect(Collectors.toList()));
                    throw new IllegalStateException();
                }
            } catch (InterruptedException | ExecutionException e) {
                log.error("Failed to see into the future :(");
            }
            return Futures.transform(unmerged, mergeUsing(application, activeAnnotations));
        }
    }

    private Function<ResolvedEquivalents<E>, ResolvedEquivalents<E>> mergeUsing(
            final Application application, Set<Annotation> activeAnnotations) {
        return input -> {

            ResolvedEquivalents.Builder<E> builder = ResolvedEquivalents.builder();
            for (Map.Entry<Id, Collection<E>> entry : input.asMap().entrySet()) {
                builder.putEquivalents(
                        entry.getKey(),
                        merge(entry.getKey(), entry.getValue(), application, activeAnnotations)
                );
            }
            return builder.build();
        };
    }

    private Iterable<E> merge(Id id, Collection<E> equivs, Application application, Set<Annotation> activeAnnotations) {
        return merger.merge(Optional.of(id), equivs, application, activeAnnotations);
    }
}