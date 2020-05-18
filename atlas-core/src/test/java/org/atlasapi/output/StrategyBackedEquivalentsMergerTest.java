package org.atlasapi.output;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import org.atlasapi.annotation.Annotation;
import org.atlasapi.content.Brand;
import org.atlasapi.content.Content;
import org.atlasapi.entity.Id;
import org.atlasapi.media.entity.Publisher;

import com.metabroadcast.applications.client.model.internal.Application;
import com.metabroadcast.applications.client.model.internal.ApplicationConfiguration;

import com.google.common.base.Optional;
import com.google.common.collect.Collections2;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Matchers;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import static org.hamcrest.Matchers.any;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.anyCollectionOf;
import static org.mockito.Matchers.argThat;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class StrategyBackedEquivalentsMergerTest {

    @Mock private EquivalentsMergeStrategy<Content> strategy;
    @Mock private Application nonMergingApplication;
    @Mock private Application mergingApplication;

    private StrategyBackedEquivalentsMerger<Content> merger;

    @Before
    public void setUp() {
        merger = new StrategyBackedEquivalentsMerger<>(strategy);

        when(nonMergingApplication.getConfiguration())
                .thenReturn(getConfig(ImmutableList.of()));

        when(mergingApplication.getConfiguration())
                .thenReturn(getConfig(ImmutableList.of(Publisher.BBC, Publisher.TED)));
    }

    @Test
    public void testDoesntMergeForNonMergingConfig() {
        Id id = Id.valueOf(1234);
        List<Content> merged = merger.merge(Optional.of(id), ImmutableSet.of(),
                nonMergingApplication, Collections.emptySet()
        );

        assertTrue(merged.isEmpty());
        veryifyNoMerge(nonMergingApplication);
    }


    @Test
    public void testDoesntMergeForEmptyEquivalenceSet() {
        Id id = Id.valueOf(1234);
        List<Content> merged = merger.merge(Optional.of(id), ImmutableSet.<Content>of(),
                mergingApplication, Collections.emptySet()
        );

        assertTrue(merged.isEmpty());
        veryifyNoMerge(mergingApplication);
    }

    @Test
    public void testDoesntMergeForSingletonEquivalenceSet() {
        Content brand = new Brand(Id.valueOf(1), Publisher.BBC);
        when(strategy.merge(ImmutableList.of(brand), mergingApplication, Collections.emptySet())).thenReturn(brand);
        List<Content> merged = merger.merge(Optional.of(brand.getId()), ImmutableSet.of(brand),
                mergingApplication, Collections.emptySet()
        );

        assertThat(merged.size(), is(1));
    }

    private void veryifyNoMerge(Application application) {
        verify(strategy, never()).merge(
                anyCollectionOf(Content.class),
                argThat(is(application)),
                Matchers.anySetOf(Annotation.class)
        );
    }

    //TODO test by going thru permutations of contents, and results will be the same regardless of order
    //TODO add and check both URIs and IDs

    @Test
    @SuppressWarnings("unchecked")
    public void testMergeSortingIsStable() {

        Content one = new Brand(Id.valueOf(1), Publisher.BBC);
        one.setCanonicalUri("one");
        Content two = new Brand(Id.valueOf(2), Publisher.BBC);
        two.setCanonicalUri("two");
        Content three = new Brand(Id.valueOf(3), Publisher.TED);
        three.setCanonicalUri("three");

        ImmutableList<Content> contents = ImmutableList.of(one, two, three);

        for (List<Content> contentList : Collections2.permutations(contents)) {

            when(strategy.merge(
                    anyCollectionOf(Content.class),
                    argThat(is(mergingApplication)),
                    Matchers.anySetOf(Annotation.class)
            )).thenReturn(one);

            List<Content> merged = merger.merge(Optional.of(one.getId()), contentList, mergingApplication,
                    Collections.emptySet());

            Content mergedBrand = Iterables.getOnlyElement(merged);
            assertThat(mergedBrand.getCanonicalUri(), is(one.getCanonicalUri()));
            assertThat(mergedBrand.getId(), is(one.getId()));

            if (contentList.get(0).equals(one)) {
                verify(strategy)
                        .merge(
                                argThat(contains(one, two, three)),
                                argThat(is(mergingApplication)),
                                Matchers.anySetOf(Annotation.class)
                        );
            } else if (contentList.get(0).equals(two)) {
                verify(strategy)
                        .merge(
                                argThat(contains(one, two, three)),
                                argThat(is(mergingApplication)),
                                Matchers.anySetOf(Annotation.class)
                        );
            } else {
                verify(strategy)
                        .merge(
                                argThat(contains(one, two, three)),
                                argThat(is(mergingApplication)),
                                Matchers.anySetOf(Annotation.class)
                        );
            }

            reset(strategy);
        }
    }

    @Test
    public void worksWithMultipleItemsFromSamePublisherRetrieved() {

        Content retrieved1 = new Brand(Id.valueOf(1), Publisher.BBC_KIWI);
        Content retrieved2 = new Brand(Id.valueOf(2), Publisher.BBC_KIWI);

        when(strategy.merge(
                anyCollectionOf(Content.class),
                argThat(is(mergingApplication)),
                Matchers.anySetOf(Annotation.class)
        )).thenReturn(retrieved1);

        merger.merge(
                Optional.of(retrieved1.getId()),
                ImmutableList.of(retrieved1, retrieved2),
                mergingApplication,
                Collections.emptySet()
        );

        verify(strategy)
                .merge(
                        argThat(contains(retrieved1, retrieved2)),
                        argThat(is(mergingApplication)),
                        Matchers.anySetOf(Annotation.class)
                );
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testMergeVictimIsRequestedContentIdIfVictimMatchesMostPrecedentSource() {
        Content one = new Brand(Id.valueOf(1), Publisher.BBC);
        Content two = new Brand(Id.valueOf(2), Publisher.BBC);
        Content three = new Brand(Id.valueOf(3), Publisher.TED);

        setUpMockStrategyToReturn(one);
        List<Content> merged = merger.merge(
                Optional.of(one.getId()),
                ImmutableSet.of(one, two, three),
                mergingApplication,
                Collections.emptySet()
        );

        verify(strategy)
                .merge(
                        argThat(contains(one, two, three)),
                        argThat(is(mergingApplication)),
                        Matchers.anySetOf(Annotation.class)
                );
        reset(strategy);
        setUpMockStrategyToReturn(one);
        merged = merger.merge(Optional.of(two.getId()), ImmutableSet.of(one, two, three),
                mergingApplication, Collections.emptySet()
        );

        verify(strategy)
                .merge(
                        argThat(contains(one, two, three)),
                        argThat(is(mergingApplication)),
                        Matchers.anySetOf(Annotation.class)
                );
        reset(strategy);
        setUpMockStrategyToReturn(one);
        merged = merger.merge(Optional.of(three.getId()), ImmutableSet.of(one, two, three),
                mergingApplication, Collections.emptySet()
        );

        verify(strategy)
                .merge(
                        argThat(contains(one, two, three)),
                        argThat(is(mergingApplication)),
                        Matchers.anySetOf(Annotation.class)
                );
        reset(strategy);
    }

    private void setUpMockStrategyToReturn(Content content) {
        when(strategy.merge(
                anyCollectionOf(Content.class),
                argThat(is(mergingApplication)),
                Matchers.anySetOf(Annotation.class)
        )).thenReturn(content);
    }

    private ApplicationConfiguration getConfig(List<Publisher> publishers) {
        List<Publisher> finalPublishers = ImmutableList.<Publisher>builder()
                .addAll(
                        Publisher.all().stream()
                                .filter(Publisher::enabledWithNoApiKey)
                                .filter(publisher -> !publishers.contains(publisher))
                                .collect(Collectors.toList())
                )
                .addAll(publishers)
                .build();

        return ApplicationConfiguration.builder()
                .withPrecedence(finalPublishers)
                .withEnabledWriteSources(ImmutableList.of())
                .build();
    }

}
