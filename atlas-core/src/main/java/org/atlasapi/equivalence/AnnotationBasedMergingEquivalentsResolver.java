package org.atlasapi.equivalence;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

import org.atlasapi.annotation.Annotation;
import org.atlasapi.application.ApplicationSources;
import org.atlasapi.content.Content;
import org.atlasapi.entity.Id;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

public class AnnotationBasedMergingEquivalentsResolver<E extends Equivalable<E>>
        implements MergingEquivalentsResolver<E> {
    
    private final EquivalentsResolver<E> resolver;
    private final ApplicationEquivalentsMerger<E> merger;

    public AnnotationBasedMergingEquivalentsResolver(EquivalentsResolver<E> resolver, ApplicationEquivalentsMerger<E> merger) {
        this.resolver = checkNotNull(resolver);
        this.merger = checkNotNull(merger);
    }
    
    @Override
    public ListenableFuture<ResolvedEquivalents<E>> resolveIds(Iterable<Id> ids,
            ApplicationSources sources, Set<Annotation> activeAnnotations) {
        ListenableFuture<ResolvedEquivalents<E>> unmerged
            = resolver.resolveIds(ids, sources.getEnabledReadSources(), activeAnnotations);

        if(activeAnnotations.contains(Annotation.NON_MERGED)) {
            return Futures.transform(unmerged, getOnlyRequested(ids));
        } else {
            return Futures.transform(unmerged, mergeUsing(sources));
        }
    }

    private Function<ResolvedEquivalents<E>, ResolvedEquivalents<E>> mergeUsing(
            final ApplicationSources sources) {
        return new Function<ResolvedEquivalents<E>, ResolvedEquivalents<E>>() {
            @Override
            public ResolvedEquivalents<E> apply(ResolvedEquivalents<E> input) {
                
                ResolvedEquivalents.Builder<E> builder = ResolvedEquivalents.builder();
                for (Map.Entry<Id, Collection<E>> entry : input.asMap().entrySet()) {
                    builder.putEquivalents(entry.getKey(), merge(entry.getKey(), entry.getValue(), sources));
                }
                return builder.build();
            }
        };
    }

    private Function<ResolvedEquivalents<E>, ResolvedEquivalents<E>> getOnlyRequested(
            final Iterable<Id> ids) {
        return new Function<ResolvedEquivalents<E>, ResolvedEquivalents<E>>() {
            @Override
            public ResolvedEquivalents<E> apply(ResolvedEquivalents<E> input) {

                ResolvedEquivalents.Builder<E> builder = ResolvedEquivalents.builder();
                for (Map.Entry<Id, Collection<E>> entry : input.asMap().entrySet()) {
                    for (Object e : entry.getValue()) {
                        if (e instanceof Content) {
                            if (Iterables.contains(ids, ((Content) e).getId())){
                                builder.putEquivalents(entry.getKey(), entry.getValue());
                            }
                        }
                    }
                }
                return builder.build();
            }
        };
    }

    private Iterable<E> merge(Id id, Collection<E> equivs, ApplicationSources sources) {
        return merger.merge(Optional.of(id), equivs, sources);
    }
}