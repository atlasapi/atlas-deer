package org.atlasapi.output;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import java.util.List;

import com.google.common.collect.ImmutableMap;
import org.atlasapi.application.ApplicationSources;
import org.atlasapi.application.SourceReadEntry;
import org.atlasapi.application.SourceStatus;
import org.atlasapi.content.Brand;
import org.atlasapi.content.BroadcastRef;
import org.atlasapi.content.Container;
import org.atlasapi.content.Content;
import org.atlasapi.content.Item;
import org.atlasapi.content.ItemRef;
import org.atlasapi.entity.Id;
import org.atlasapi.equivalence.EquivalenceRef;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.segment.SegmentEvent;
import org.atlasapi.segment.SegmentRef;
import org.hamcrest.Matchers;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Duration;
import org.joda.time.Interval;
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

    @Test
    public void testMergedContentHasCorrectSegmentEvents() {
        Item one = item(1L, "one", Publisher.BBC_KIWI);
        Item two = item(2L, "two", Publisher.BBC_KIWI);
        Item three = item(3L, "three", Publisher.BBC_MUSIC);

        setEquivalent(one, two, three);
        setEquivalent(two, one, three);
        setEquivalent(three, two, one);

        SegmentEvent seOne = new SegmentEvent();
        seOne.setId(Id.valueOf(10L));
        seOne.setPublisher(Publisher.BBC_KIWI);

        SegmentEvent seTwo = new SegmentEvent();
        seTwo.setId(Id.valueOf(20L));
        seTwo.setPublisher(Publisher.BBC_KIWI);

        SegmentEvent seThree = new SegmentEvent();
        seThree.setId(Id.valueOf(30L));
        seThree.setPublisher(Publisher.BBC_KIWI);


        SegmentEvent seFour = new SegmentEvent();
        seFour.setId(Id.valueOf(40L));
        seFour.setPublisher(Publisher.BBC_MUSIC);


        SegmentEvent seFive = new SegmentEvent();
        seFive.setId(Id.valueOf(50L));
        seFive.setPublisher(Publisher.BBC_MUSIC);

        one.setSegmentEvents(ImmutableList.of(seOne, seTwo));
        two.setSegmentEvents(ImmutableList.of(seThree));
        three.setSegmentEvents(ImmutableList.of(seFour, seFive));

        ApplicationSources sources = sourcesWithPrecedence(Publisher.BBC_KIWI, Publisher.BBC_MUSIC);

        Item merged = merger.merge(one, ImmutableList.of(two, three), sources);

        List<SegmentEvent> mergedSegmentEvents = merged.getSegmentEvents();

        assertThat(mergedSegmentEvents, Matchers.<List<SegmentEvent>>is(ImmutableList.of(seOne, seTwo, seFour, seFive)));

    }

    @Test
    public void testMergedContentHasCorrectUpcomingContent() {
        Container one = brand(1L, "one", Publisher.BBC_KIWI);
        Container two = brand(2L, "two", Publisher.METABROADCAST);
        Container three = brand(3L, "three", Publisher.BBC_MUSIC);

        Item item1 = item(4L, "item", Publisher.METABROADCAST);
        item1.setThisOrChildLastUpdated(DateTime.now(DateTimeZone.UTC));

        setEquivalent(one, two, three);
        setEquivalent(two, one, three);
        setEquivalent(three, two, one);

        ImmutableMap<ItemRef, Iterable<BroadcastRef>> upcomingContent = ImmutableMap.<ItemRef, Iterable<BroadcastRef>>builder()
                .put(
                        item1.toRef(),
                        ImmutableList.of(
                                new BroadcastRef(
                                        "broadcast1",
                                        Id.valueOf(5),
                                        new Interval(
                                                DateTime.now(DateTimeZone.UTC).plusHours(1),
                                                DateTime.now(DateTimeZone.UTC).plusHours(2)
                                        )
                                )
                        )
                ).build();

        one.setUpcomingContent(
                upcomingContent
        );

        ApplicationSources sources = sourcesWithPrecedence(Publisher.BBC_KIWI,Publisher.METABROADCAST,Publisher.BBC_MUSIC);

        Container merged = merger.merge(one, ImmutableList.of(two, three), sources);

        assertThat(merged.getUpcomingContent(), is(upcomingContent));

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