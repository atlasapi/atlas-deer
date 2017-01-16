package org.atlasapi.output;

import java.time.ZonedDateTime;
import java.util.List;

import com.google.common.collect.Lists;
import com.metabroadcast.applications.client.model.internal.AccessRoles;
import com.metabroadcast.applications.client.model.internal.Application;
import com.metabroadcast.applications.client.model.internal.ApplicationConfiguration;
import com.metabroadcast.applications.client.model.internal.Environment;
import org.atlasapi.application.DefaultApplication;
import org.atlasapi.content.Brand;
import org.atlasapi.content.Content;
import org.atlasapi.entity.Id;
import org.atlasapi.media.entity.Publisher;

import com.google.common.base.Optional;
import com.google.common.collect.Collections2;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import static org.hamcrest.Matchers.any;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.anyCollectionOf;
import static org.mockito.Matchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class StrategyBackedEquivalentsMergerTest {

    @Mock private EquivalentsMergeStrategy<Content> strategy;

    private StrategyBackedEquivalentsMerger<Content> merger;

    @Before
    public void setup() {
        merger = new StrategyBackedEquivalentsMerger<Content>(strategy);
    }

    private final Application nonMergingApplication = DefaultApplication.create();
    private final Application mergingApplication = Application.builder()
            .withId(-1l)
            .withTitle("test")
            .withDescription("desc")
            .withEnvironment(mock(Environment.class))
            .withCreated(ZonedDateTime.now())
            .withApiKey("test")
            .withSources(ApplicationConfiguration.builder()
                    .withPrecedence(Lists.newArrayList(Publisher.BBC, Publisher.TED))
                    .withEnabledWriteSources(Lists.newArrayList())
                    .build())
            .withAllowedDomains(Lists.newArrayList())
            .withAccessRoles(mock(AccessRoles.class))
            .withRevoked(false)
            .build();

    @Test
    public void testDoesntMergeForNonMergingConfig() {
        Id id = Id.valueOf(1234);
        List<Content> merged = merger.merge(Optional.of(id), ImmutableSet.of(),
                nonMergingApplication
        );

        assertTrue(merged.isEmpty());
        veryifyNoMerge(nonMergingApplication);
    }

    @Test
    public void testDoesntMergeForEmptyEquivalenceSet() {
        Id id = Id.valueOf(1234);
        List<Content> merged = merger.merge(Optional.of(id), ImmutableSet.<Content>of(),
                mergingApplication
        );

        assertTrue(merged.isEmpty());
        veryifyNoMerge(mergingApplication);
    }

    @Test
    public void testDoesntMergeForSingletonEquivalenceSet() {
        Content brand = new Brand(Id.valueOf(1), Publisher.BBC);
        when(strategy.merge(brand, ImmutableList.of(), mergingApplication)).thenReturn(brand);
        List<Content> merged = merger.merge(Optional.of(brand.getId()), ImmutableSet.of(brand),
                mergingApplication
        );

        assertThat(merged.size(), is(1));
    }

    private void veryifyNoMerge(Application application) {
        verify(strategy, never()).merge(
                argThat(any(Content.class)),
                anyCollectionOf(Content.class),
                argThat(is(application))
        );
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testMergeSortingIsStable() {

        Content one = new Brand(Id.valueOf(1), Publisher.BBC);
        Content two = new Brand(Id.valueOf(2), Publisher.BBC);
        Content three = new Brand(Id.valueOf(3), Publisher.TED);

        ImmutableList<Content> contents = ImmutableList.of(one, two, three);

        for (List<Content> contentList : Collections2.permutations(contents)) {

            when(strategy.merge(
                    argThat(any(Content.class)),
                    anyCollectionOf(Content.class),
                    argThat(is(mergingApplication))
            )).thenReturn(one);

            merger.merge(Optional.of(one.getId()), contentList, mergingApplication);

            if (contentList.get(0).equals(one)) {
                verify(strategy)
                        .merge(
                                argThat(is(one)),
                                argThat(contains(two, three)),
                                argThat(is(mergingApplication))
                        );
            } else if (contentList.get(0).equals(two)) {
                verify(strategy)
                        .merge(
                                argThat(is(one)),
                                argThat(contains(two, three)),
                                argThat(is(mergingApplication))
                        );
            } else {
                verify(strategy)
                        .merge(
                                argThat(is(one)),
                                argThat(contains(two, three)),
                                argThat(is(mergingApplication))
                        );
            }

            reset(strategy);
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testMergeVictimIsRequestedContentIdIfVictimMatchesMostPrecedentSource() {
        Content one = new Brand(Id.valueOf(1), Publisher.BBC);
        Content two = new Brand(Id.valueOf(2), Publisher.BBC);
        Content three = new Brand(Id.valueOf(3), Publisher.TED);

        setUpMockStrategyToReturn(one);
        List<Content> merged = merger.merge(Optional.of(one.getId()),
                ImmutableSet.of(one, two, three),
                mergingApplication
        );

        verify(strategy)
                .merge(
                        argThat(is(one)),
                        argThat(contains(two, three)),
                        argThat(is(mergingApplication))
                );
        reset(strategy);
        setUpMockStrategyToReturn(one);
        merged = merger.merge(Optional.of(two.getId()), ImmutableSet.of(one, two, three),
                mergingApplication
        );

        verify(strategy)
                .merge(
                        argThat(is(two)),
                        argThat(contains(one, three)),
                        argThat(is(mergingApplication))
                );
        reset(strategy);
        setUpMockStrategyToReturn(one);
        merged = merger.merge(Optional.of(three.getId()), ImmutableSet.of(one, two, three),
                mergingApplication
        );

        verify(strategy)
                .merge(
                        argThat(is(one)),
                        argThat(contains(two, three)),
                        argThat(is(mergingApplication))
                );
        reset(strategy);
    }

    private void setUpMockStrategyToReturn(Content content) {
        when(strategy.merge(
                argThat(any(Content.class)),
                anyCollectionOf(Content.class),
                argThat(is(mergingApplication))
        )).thenReturn(content);
    }

}
