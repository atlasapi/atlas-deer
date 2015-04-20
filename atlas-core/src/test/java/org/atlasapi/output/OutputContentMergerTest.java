package org.atlasapi.output;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import java.util.List;

import org.atlasapi.application.ApplicationSources;
import org.atlasapi.application.SourceReadEntry;
import org.atlasapi.application.SourceStatus;
import org.atlasapi.content.BlackoutRestriction;
import org.atlasapi.content.Brand;
import org.atlasapi.content.Broadcast;
import org.atlasapi.content.Content;
import org.atlasapi.content.Item;
import org.atlasapi.entity.Id;
import org.atlasapi.equivalence.EquivalenceRef;
import org.atlasapi.media.entity.Publisher;
import org.joda.time.DateTime;
import org.junit.Test;

import com.google.common.base.Function;
import com.google.common.collect.Collections2;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;


public class OutputContentMergerTest {

    private final OutputContentMerger merger = new OutputContentMerger();
    
    @Test
    public void testSortOfCommonSourceContentIsStable() {
        Brand one = brand(1L, "one",Publisher.BBC);
        Brand two = brand(2L, "two",Publisher.BBC);
        Brand three = brand(3L, "three",Publisher.TED);
        
        setEquivalent(one, two, three);
        setEquivalent(two, one, three);
        setEquivalent(three, two, one);
        
        ApplicationSources sources = sourcesWithPrecedence(Publisher.BBC, Publisher.TED);
        
        ImmutableList<Brand> contents = ImmutableList.of(one, two, three);
        
        for (List<Brand> contentList : Collections2.permutations(contents)) {
            List<Brand> merged = merger.merge(sources, contentList);
            assertThat(merged.size(), is(1));
            if (contentList.get(0).equals(three)) {
                assertThat(contentList.toString(), merged.get(0), is(contentList.get(1)));
            } else {
                assertThat(contentList.toString(), merged.get(0), is(contentList.get(0)));
            }
        }

    }

    @Test
    public void testMergedContentHasLowestIdOfContentInEquivalenceSet() {
        
        Brand one = brand(5L, "one", Publisher.BBC);
        Brand two = brand(2L, "two",Publisher.PA);
        Brand three = brand(10L, "three",Publisher.TED);
        
        setEquivalent(one, two, three);
        setEquivalent(two, one, three);
        setEquivalent(three, two, one);
        
        //two is intentionally missing here
        ImmutableList<Brand> contents = ImmutableList.of(one, three);

        ApplicationSources sources = sourcesWithPrecedence(Publisher.BBC, Publisher.TED);
        mergePermutations(contents, sources, one, two.getId());
        
        one = brand(5L, "one", Publisher.BBC);
        two = brand(2L, "two",Publisher.PA);
        three = brand(10L, "three",Publisher.TED);
        
        setEquivalent(one, two, three);
        setEquivalent(two, one, three);
        setEquivalent(three, two, one);

        contents = ImmutableList.of(one, three);
        
        sources = sourcesWithPrecedence(Publisher.TED,Publisher.BBC);
        mergePermutations(contents, sources, three, two.getId());
        
    }

    private Brand brand(long id, String uri, Publisher source) {
        Brand one = new Brand(uri,uri,source);
        one.setId(id);
        return one;
    }

    private Item item(long id, String uri, Publisher source) {
        Item one = new Item(uri,uri,source);
        one.setId(id);
        return one;
    }

    private void mergePermutations(ImmutableList<Brand> contents, ApplicationSources sources,
            Brand expectedContent, Id expectedId) {
        for (List<Brand> contentList : Collections2.permutations(contents)) {
            List<Brand> merged = merger.merge(sources, contentList);
            Brand mergedBrand = Iterables.getOnlyElement(merged);
            assertThat(mergedBrand, is(expectedContent));
            assertThat(mergedBrand.getId(), is(expectedId));
        }
    }
    
    private ApplicationSources sourcesWithPrecedence(Publisher...publishers) {
        return ApplicationSources.defaults().copy().withPrecedence(true)
                .withReadableSources(Lists.transform(ImmutableList.copyOf(publishers),
                    new Function<Publisher, SourceReadEntry>() {

                        @Override
                        public SourceReadEntry apply(Publisher input) {
                            return new SourceReadEntry(input, SourceStatus.AVAILABLE_ENABLED);
                        }
                    }
                ))
                .build();
    }
    
    private void setEquivalent(Content receiver, Content...equivalents) {
        ImmutableList<Content> allContent = ImmutableList.<Content>builder()
            .add(receiver)
            .addAll(ImmutableList.copyOf(equivalents))
            .build();
        receiver.setEquivalentTo(ImmutableSet.copyOf(Iterables.transform(
            allContent, EquivalenceRef.toEquivalenceRef())
        ));
    }
    
}