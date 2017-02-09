package org.atlasapi.content;

import java.util.Set;
import java.util.concurrent.TimeUnit;

import com.metabroadcast.applications.client.model.internal.Application;
import org.atlasapi.annotation.Annotation;
import org.atlasapi.entity.Id;
import org.atlasapi.equivalence.MergingEquivalentsResolver;
import org.atlasapi.equivalence.ResolvedEquivalents;

import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.Futures;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkNotNull;

public class MergingEquivalentsResolverBackedContainerSummaryResolver
        implements ContainerSummaryResolver {

    private static final Logger log = LoggerFactory.getLogger(
            MergingEquivalentsResolverBackedContainerSummaryResolver.class);
    private final MergingEquivalentsResolver<Content> contentResolver;

    public MergingEquivalentsResolverBackedContainerSummaryResolver(
            MergingEquivalentsResolver<Content> contentResolver) {
        this.contentResolver = checkNotNull(contentResolver);
    }

    @Override
    public Optional<ContainerSummary> resolveContainerSummary(Id id,
            Application application, Set<Annotation> annotations) {
        ResolvedEquivalents<Content> contentResolved = null;
        try {
            contentResolved = Futures.get(
                    contentResolver.resolveIds(
                            ImmutableSet.of(id),
                            application,
                            annotations
                    ),
                    1, TimeUnit.MINUTES,
                    Exception.class
            );
        } catch (Exception e) {
            Throwables.propagate(e);
        }
        Set<Content> equivalentContent = contentResolved.get(id);
        if (equivalentContent.isEmpty()) {
            log.warn("Reference to non-existent container with ID {}", id);
            return Optional.absent();
        }

        Content content = Iterables.getFirst(equivalentContent, null);
        if (content instanceof Container) {
            return Optional.of((((Container) content).toSummary()));
        }

        log.warn(
                "Incorrect type of content with ID {}, expected Container, got {}",
                id,
                content.getClass().getName()
        );
        return Optional.absent();
    }
}
