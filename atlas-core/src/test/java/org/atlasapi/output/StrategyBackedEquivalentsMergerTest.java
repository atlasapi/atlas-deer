package org.atlasapi.output;

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

import java.util.List;

import org.atlasapi.application.ApplicationSources;
import org.atlasapi.application.SourceReadEntry;
import org.atlasapi.application.SourceStatus;
import org.atlasapi.content.Brand;
import org.atlasapi.content.Content;
import org.atlasapi.entity.Id;
import org.atlasapi.media.entity.Publisher;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.google.common.base.Optional;
import com.google.common.collect.Collections2;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

@RunWith(MockitoJUnitRunner.class)
public class StrategyBackedEquivalentsMergerTest {

    @Mock private EquivalentsMergeStrategy<Content> strategy;
    
    private StrategyBackedEquivalentsMerger<Content> merger;
    
    @Before
    public void setup() {
        merger = new StrategyBackedEquivalentsMerger<Content>(strategy);
    }
    
    private final ApplicationSources nonMergingSources = ApplicationSources.defaults()
            .copy().withPrecedence(false).build();
    private final ApplicationSources mergingSources = ApplicationSources.defaults()
            .copy().withPrecedence(true)
            .withReadableSources(ImmutableList.of(
                    new SourceReadEntry(Publisher.BBC, SourceStatus.AVAILABLE_ENABLED),
                    new SourceReadEntry(Publisher.TED, SourceStatus.AVAILABLE_ENABLED)
             ))
            .build();
    
    @Test
    public void testDoesntMergeForNonMergingConfig() {
        Id id = Id.valueOf(1234);
        List<Content> merged = merger.merge(Optional.of(id), ImmutableSet.<Content>of(), 
                nonMergingSources);
        
        assertTrue(merged.isEmpty());
        veryifyNoMerge(nonMergingSources);
    }
    
    @Test
    public void testDoesntMergeForEmptyEquivalenceSet() {
        Id id = Id.valueOf(1234);
        List<Content> merged = merger.merge(Optional.of(id), ImmutableSet.<Content>of(), 
                mergingSources);
        
        assertTrue(merged.isEmpty());
        veryifyNoMerge(mergingSources);
    }

    @Test
    public void testDoesntMergeForSingletonEquivalenceSet() {
        Content brand = new Brand(Id.valueOf(1), Publisher.BBC);
        List<Content> merged = merger.merge(Optional.of(brand.getId()), ImmutableSet.of(brand), 
                mergingSources);
        
        assertThat(merged.size(), is(1));
        veryifyNoMerge(mergingSources);
    }

    private void veryifyNoMerge(ApplicationSources sources) {
        verify(strategy, never()).merge(
            argThat(any(Content.class)), 
            anyCollectionOf(Content.class), 
            argThat(is(sources))
        );
    }
    
    @Test
    @SuppressWarnings("unchecked")
    public void testMergeSortingIsStable() {
        
        Content one = new Brand(Id.valueOf(1),Publisher.BBC);
        Content two = new Brand(Id.valueOf(2),Publisher.BBC);
        Content three = new Brand(Id.valueOf(3),Publisher.TED);
        
        ImmutableList<Content> contents = ImmutableList.of(one, two, three);
        
        for (List<Content> contentList : Collections2.permutations(contents)) {
            
            when(strategy.merge(
                argThat(any(Content.class)), 
                anyCollectionOf(Content.class), 
                argThat(is(mergingSources))
            )).thenReturn(one);
            
            merger.merge(Optional.of(one.getId()), contentList, mergingSources);
            
            if (contentList.get(0).equals(one)) {
                verify(strategy)
                    .merge(argThat(is(one)), argThat(contains(two, three)), argThat(is(mergingSources)));
            } else if (contentList.get(0).equals(two)) {
                verify(strategy)
                    .merge(argThat(is(one)), argThat(contains(two, three)), argThat(is(mergingSources)));
            } else {
                verify(strategy)
                    .merge(argThat(is(one)), argThat(contains(two, three)), argThat(is(mergingSources)));
            }
            
            reset(strategy);
        }
    }
    
    @Test
    @SuppressWarnings("unchecked")
    public void testMergeVictimIsRequestedContentIdIfVictimMatchesMostPrecedentSource() {
        Content one = new Brand(Id.valueOf(1),Publisher.BBC);
        Content two = new Brand(Id.valueOf(2),Publisher.BBC);
        Content three = new Brand(Id.valueOf(3),Publisher.TED);
        
        setUpMockStrategyToReturn(one);
        List<Content> merged = merger.merge(Optional.of(one.getId()), ImmutableSet.of(one, two, three), 
                mergingSources);
        
        verify(strategy)
            .merge(argThat(is(one)), argThat(contains(two, three)), argThat(is(mergingSources)));
        reset(strategy);
        setUpMockStrategyToReturn(one);
        merged = merger.merge(Optional.of(two.getId()), ImmutableSet.of(one, two, three), 
                mergingSources);
       
        verify(strategy)
            .merge(argThat(is(two)), argThat(contains(one, three)), argThat(is(mergingSources)));
        reset(strategy);
        setUpMockStrategyToReturn(one); 
        merged = merger.merge(Optional.of(three.getId()), ImmutableSet.of(one, two, three), 
                mergingSources);
        
        verify(strategy)
            .merge(argThat(is(one)), argThat(contains(two, three)), argThat(is(mergingSources)));
        reset(strategy);
    }
    
    private void setUpMockStrategyToReturn(Content content) {
        when(strategy.merge(
                argThat(any(Content.class)), 
                anyCollectionOf(Content.class), 
                argThat(is(mergingSources))
            )).thenReturn(content);
    }

}
