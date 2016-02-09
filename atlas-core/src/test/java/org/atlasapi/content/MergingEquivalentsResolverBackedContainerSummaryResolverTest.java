package org.atlasapi.content;

import org.atlasapi.annotation.Annotation;
import org.atlasapi.application.ApplicationSources;
import org.atlasapi.entity.Id;
import org.atlasapi.equivalence.AnnotationBasedMergingEquivalentsResolver;
import org.atlasapi.equivalence.ApplicationEquivalentsMerger;
import org.atlasapi.equivalence.EquivalenceRef;
import org.atlasapi.equivalence.EquivalentsResolver;
import org.atlasapi.equivalence.MergingEquivalentsResolver;
import org.atlasapi.equivalence.ResolvedEquivalents;
import org.atlasapi.media.entity.Publisher;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Matchers;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class MergingEquivalentsResolverBackedContainerSummaryResolverTest {

    @Mock
    private MergingEquivalentsResolver<Content> contentResolver;
    private AnnotationBasedMergingEquivalentsResolver<Content> contentAnnotationBasedMergingEquivalentsResolver;
    @InjectMocks
    private MergingEquivalentsResolverBackedContainerSummaryResolver objectUnderTest;

    @Test
    public void testResolveContainerSummary() throws Exception {
        Id id = Id.valueOf(1L);
        ApplicationSources applicationSources = mock(ApplicationSources.class);
        Iterable<Id> containerIds = ImmutableSet.of(id);
        ContainerSummary containerSummary = mock(ContainerSummary.class);
        Container container = mock(Container.class);
        when(container.getSource()).thenReturn(Publisher.BBC);
        when(container.getId()).thenReturn(id);
        when(container.copyWithEquivalentTo(Matchers.<Iterable<EquivalenceRef>>any())).thenReturn(
                container);
        when(container.toSummary()).thenReturn(containerSummary);

        ResolvedEquivalents<Content> resolvedEquivalents = ResolvedEquivalents.<Content>builder()
                .putEquivalents(id, ImmutableSet.<Content>of(container)
                ).build();

        ListenableFuture<ResolvedEquivalents<Content>> resolved = Futures.immediateFuture(
                resolvedEquivalents
        );

        when(contentResolver.resolveIds(
                containerIds,
                applicationSources,
                ImmutableSet.of()
        )).thenReturn(resolved);

        Optional<ContainerSummary> containerSummaryOptional = objectUnderTest.resolveContainerSummary(
                id,
                applicationSources,
                ImmutableSet.of()
        );

        assertThat(containerSummaryOptional.get(), is(containerSummary));
    }

    @Test
    public void testReturnAbsentWhenContainerDoesNotExist() throws Exception {
        Id id = Id.valueOf(1L);
        Iterable<Id> containerIds = ImmutableSet.of(id);
        ListenableFuture<ResolvedEquivalents<Content>> resolved = Futures.immediateFuture(
                ResolvedEquivalents.<Content>empty()
        );
        ApplicationSources applicationSources = mock(ApplicationSources.class);

        when(contentResolver.resolveIds(
                containerIds,
                applicationSources,
                ImmutableSet.of()
        )).thenReturn(resolved);

        Optional<ContainerSummary> containerSummaryOptional = objectUnderTest.resolveContainerSummary(
                id,
                applicationSources,
                ImmutableSet.of()
        );

        assertThat(containerSummaryOptional.isPresent(), is(false));
    }

    @Test
    public void testReturnAbsentWhenReferencedContentIsNotAContainer() throws Exception {
        Id id = Id.valueOf(1L);
        Iterable<Id> containerIds = ImmutableSet.of(id);
        Content nonContainer = mock(Content.class);
        when(nonContainer.getSource()).thenReturn(Publisher.BBC);
        when(nonContainer.getId()).thenReturn(id);
        when(nonContainer.copyWithEquivalentTo(Matchers.<Iterable<EquivalenceRef>>any())).thenReturn(
                nonContainer);

        ApplicationSources applicationSources = mock(ApplicationSources.class);

        ResolvedEquivalents<Content> resolvedEquivalents = ResolvedEquivalents.<Content>builder()
                .putEquivalents(id, ImmutableSet.of(nonContainer)
                ).build();

        ListenableFuture<ResolvedEquivalents<Content>> resolved = Futures.immediateFuture(
                resolvedEquivalents
        );

        when(contentResolver.resolveIds(
                containerIds,
                applicationSources,
                ImmutableSet.of()
        )).thenReturn(resolved);

        Optional<ContainerSummary> containerSummaryOptional = objectUnderTest.resolveContainerSummary(
                id,
                applicationSources,
                ImmutableSet.of()
        );

        assertThat(containerSummaryOptional.isPresent(), is(false));
    }

    @Test
    public void testAnnotationBasedMErger() throws Exception {
        EquivalentsResolver<Content> resolver = mock(EquivalentsResolver.class);
        ApplicationEquivalentsMerger<Content> merger = mock(ApplicationEquivalentsMerger.class);
        ApplicationSources sources = mock(ApplicationSources.class);
        contentAnnotationBasedMergingEquivalentsResolver = new AnnotationBasedMergingEquivalentsResolver<>(
                resolver,
                merger
        );
        contentAnnotationBasedMergingEquivalentsResolver.resolveIds(
                ImmutableSet.of(),
                sources,
                ImmutableSet.of(
                        Annotation.NON_MERGED)
        );
        verify(resolver).resolveIdsWithoutEquivalence(ImmutableSet.of(), null, ImmutableSet.of(
                Annotation.NON_MERGED));
    }
}