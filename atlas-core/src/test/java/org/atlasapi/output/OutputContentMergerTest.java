package org.atlasapi.output;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;

import java.util.List;
import java.util.Map;

import org.atlasapi.application.ApplicationSources;
import org.atlasapi.application.SourceReadEntry;
import org.atlasapi.application.SourceStatus;
import org.atlasapi.content.Brand;
import org.atlasapi.content.BroadcastRef;
import org.atlasapi.content.Certificate;
import org.atlasapi.content.Container;
import org.atlasapi.content.Content;
import org.atlasapi.content.Encoding;
import org.atlasapi.content.EpisodeRef;
import org.atlasapi.content.Image;
import org.atlasapi.content.Item;
import org.atlasapi.content.ItemRef;
import org.atlasapi.content.ItemSummary;
import org.atlasapi.content.LocationSummary;
import org.atlasapi.content.SeriesRef;
import org.atlasapi.entity.Alias;
import org.atlasapi.entity.Id;
import org.atlasapi.equivalence.EquivalenceRef;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.segment.SegmentEvent;
import org.atlasapi.util.ImmutableCollectors;
import org.hamcrest.Matchers;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Interval;
import org.junit.Test;

import com.google.common.base.Function;
import com.google.common.collect.Collections2;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.metabroadcast.common.intl.Countries;


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

        ApplicationSources sources = sourcesWithPrecedence(true, Publisher.BBC, Publisher.TED);

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

        ApplicationSources sources = sourcesWithPrecedence(true, Publisher.BBC, Publisher.TED);
        mergePermutations(contents, sources, one, two.getId());

        one = brand(5L, "one", Publisher.BBC);
        two = brand(2L, "two",Publisher.PA);
        three = brand(10L, "three",Publisher.TED);

        setEquivalent(one, two, three);
        setEquivalent(two, one, three);
        setEquivalent(three, two, one);

        contents = ImmutableList.of(one, three);

        sources = sourcesWithPrecedence(true, Publisher.TED,Publisher.BBC);
        mergePermutations(contents, sources, three, two.getId());

    }
    
    @Test
    public void testMergeOfAliases() {
        Item one = item(1l, "o", Publisher.METABROADCAST);
        Item two = item(2l, "k", Publisher.BBC);
        
        one.addAlias(new Alias("a1", "v1"));
        two.addAlias(new Alias("a2", "v2"));
        
        ApplicationSources sources = sourcesWithPrecedence(true, Publisher.METABROADCAST, Publisher.BBC, Publisher.PA);
        Item merged = merger.merge(one, ImmutableList.of(two), sources);
        assertThat(merged.getAliases().size(), is(2));
    }

    @Test
    public void testSourceSetOnImagesFromParent() throws Exception {
        Item one = item(1l, "o", Publisher.METABROADCAST);
        Item two = item(2l, "k", Publisher.BBC);
        Item three = item(3l, "D", Publisher.PA);
        setEquivalent(one, two, three);
        setEquivalent(two, one, three);
        setEquivalent(three, two, one);

        one.setImages(ImmutableList.of(Image.builder("test1").build(), Image.builder("test2").build()));
        two.setImages(ImmutableList.of(Image.builder("test3").build(), Image.builder("test4").build()));

        ApplicationSources sources = sourcesWithPrecedence(true, Publisher.METABROADCAST, Publisher.BBC, Publisher.PA);
        Item merged = merger.merge(one, ImmutableList.of(two, three), sources);

        ImmutableSet<Image> images = merged.getImages().stream()
                .filter(img -> img.getSource() != null)
                .collect(ImmutableCollectors.toSet());

        assertThat(images.size(), is(2));
    }
    
    @Test
    public void testSourceSetOnImagesWhenImagePrecedenceDisabled() throws Exception {
        Item one = item(1l, "o", Publisher.METABROADCAST);
        Item two = item(2l, "k", Publisher.BBC);
        Item three = item(3l, "D", Publisher.PA);
        setEquivalent(one, two, three);
        setEquivalent(two, one, three);
        setEquivalent(three, two, one);

        one.setImages(ImmutableList.of(Image.builder("test1").build(), Image.builder("test2").build()));
        two.setImages(ImmutableList.of(Image.builder("test3").build(), Image.builder("test4").build()));

        ApplicationSources sources = sourcesWithPrecedence(false, Publisher.METABROADCAST, Publisher.BBC, Publisher.PA);
        Item merged = merger.merge(one, ImmutableList.of(two, three), sources);

        ImmutableSet<Image> images = merged.getImages().stream()
                .filter(img -> img.getSource() != null)
                .collect(ImmutableCollectors.toSet());

        assertThat(images.size(), is(4));
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

        ApplicationSources sources = sourcesWithPrecedence(true, Publisher.BBC_KIWI, Publisher.BBC_MUSIC);

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

        ApplicationSources sources = sourcesWithPrecedence(true, Publisher.BBC_KIWI,Publisher.METABROADCAST,Publisher.BBC_MUSIC);

        Container merged = merger.merge(one, ImmutableList.of(two, three), sources);

        assertThat(merged.getUpcomingContent(), is(upcomingContent));

    }

    @Test
    public void testMergedContentHasCorrectItemSummaries() {
        Container one = brand(1L, "one", Publisher.BBC_KIWI);
        Container two = brand(2L, "two", Publisher.METABROADCAST);
        Container three = brand(3L, "three", Publisher.BBC_MUSIC);

        Item item1 = item(4L, "item", Publisher.METABROADCAST);
        item1.setThisOrChildLastUpdated(DateTime.now(DateTimeZone.UTC));

        setEquivalent(one, two, three);
        setEquivalent(two, one, three);
        setEquivalent(three, two, one);

        List<ItemSummary> itemSummaries = ImmutableList.of(
                new ItemSummary(
                        new ItemRef(Id.valueOf(1), Publisher.METABROADCAST, "", DateTime.now()),
                        "",
                        "",
                        "",
                        2012,
                        ImmutableList.of(new Certificate("PG", Countries.GB))
                )
        );
        two.setItemRefs(ImmutableList.of(
                new EpisodeRef(Id.valueOf(10l), Publisher.METABROADCAST, "11", DateTime.now())
        ));
        two.setItemSummaries(
                itemSummaries
        );

        ApplicationSources sources = sourcesWithPrecedence(true, Publisher.BBC_KIWI,Publisher.METABROADCAST,Publisher.BBC_MUSIC);


        Container merged = merger.merge(one, ImmutableList.of(two, three), sources);

        assertThat(merged.getItemSummaries(), is(itemSummaries));

    }

    @Test
    public void testMergedContentHasCorrectAvailableContent() {
        Container one = brand(1L, "one", Publisher.BBC_KIWI);
        Container two = brand(2L, "two", Publisher.METABROADCAST);
        Container three = brand(3L, "three", Publisher.BBC_MUSIC);

        Item item1 = item(4L, "item", Publisher.METABROADCAST);
        item1.setThisOrChildLastUpdated(DateTime.now(DateTimeZone.UTC));

        Item item2 = item(5L, "item", Publisher.BBC_KIWI);
        item2.setThisOrChildLastUpdated(DateTime.now(DateTimeZone.UTC));

        setEquivalent(one, two, three);
        setEquivalent(two, one, three);
        setEquivalent(three, two, one);

        LocationSummary locationSummary1 = new LocationSummary(
                true,
                "broadcast1",
                DateTime.now(DateTimeZone.UTC).minusHours(1),
                DateTime.now(DateTimeZone.UTC).plusHours(1)
        );

        LocationSummary locationSummary2 = new LocationSummary(
                true,
                "broadcast2",
                DateTime.now(DateTimeZone.UTC).minusHours(1),
                DateTime.now(DateTimeZone.UTC).plusHours(1)
        );

        ImmutableMap<ItemRef, Iterable<LocationSummary>> availableContent1 = ImmutableMap.<ItemRef, Iterable<LocationSummary>>builder()
                .put(
                        item1.toRef(),
                        ImmutableList.of(
                                locationSummary1
                        )
                ).build();

        ImmutableMap<ItemRef, Iterable<LocationSummary>> availableContent2 = ImmutableMap.<ItemRef, Iterable<LocationSummary>>builder()
                .put(
                        item2.toRef(),
                        ImmutableList.of(
                                locationSummary2
                        )
                ).build();

        one.setAvailableContent(
                availableContent1
        );
        three.setAvailableContent(
                availableContent2
        );

        ApplicationSources sources = sourcesWithPrecedence(true, Publisher.BBC_KIWI, Publisher.METABROADCAST, Publisher.BBC_MUSIC);

        Container merged = merger.merge(one, ImmutableList.of(two, three), sources);

        Map<ItemRef, Iterable<LocationSummary>> expectedAvailableContent = ImmutableMap.<ItemRef, Iterable<LocationSummary>>builder()
                .putAll(availableContent1)
                .putAll(availableContent2)
                .build();

        assertThat(merged.getAvailableContent(), is(expectedAvailableContent));

    }

    @Test
    public void testMergedContentHasCorrectEncodings() {
        Container one = brand(1L, "one", Publisher.BBC_KIWI);
        Container two = brand(2L, "two", Publisher.METABROADCAST);
        Container three = brand(3L, "three", Publisher.BBC_MUSIC);


        setEquivalent(one, two, three);
        setEquivalent(two, one, three);
        setEquivalent(three, two, one);

        Encoding encoding1 = mock(Encoding.class);
        Encoding encoding2 = mock(Encoding.class);
        Encoding encoding3 = mock(Encoding.class);
        Encoding encoding4 = mock(Encoding.class);

        one.setManifestedAs(ImmutableSet.of(encoding1, encoding2));
        two.setManifestedAs(ImmutableSet.of(encoding3, encoding4));

        ApplicationSources sources = sourcesWithPrecedence(true, Publisher.BBC_KIWI, Publisher.METABROADCAST, Publisher.BBC_MUSIC);

        Container merged = merger.merge(one, ImmutableList.of(two, three), sources);

        assertThat(merged.getManifestedAs(), is(ImmutableSet.of(encoding1, encoding2, encoding3, encoding4)));

    }
    
    @Test
    public void testImageWithoutMerging() {
        ApplicationSources sources = sourcesWithPrecedence(false, Publisher.BBC, Publisher.PA);
        Item item1 = item(4L, "item1", Publisher.BBC);
        item1.setImages(ImmutableSet.of(new Image("http://image1.org/")));
        Item item2 = item(5L, "item2", Publisher.PA);
        item2.setImages(ImmutableSet.of(new Image("http://image2.org/")));
        
        Content merged = merger.merge(item1,  ImmutableList.of(item2), sources);
        assertThat(merged.getImages().size(), is(2));
    }
    
    @Test
    public void testImageWithMerging() {
        ApplicationSources sources = sourcesWithPrecedence(true, Publisher.BBC, Publisher.PA);
        Item item1 = item(4L, "item1", Publisher.BBC);
        item1.setImages(ImmutableSet.of(new Image("http://image1.org/")));
        Item item2 = item(5L, "item2", Publisher.PA);
        item2.setImages(ImmutableSet.of(new Image("http://image2.org/")));
        
        Content merged = merger.merge(item1,  ImmutableList.of(item2), sources);
        assertThat(Iterables.getOnlyElement(merged.getImages()).getCanonicalUri(), is("http://image1.org/"));
    }
    
    @Test
    public void testContentHierarchyMergingWhenNoPrecidenceDefined() {
        ApplicationSources sources = sourcesWithPrecedence(false, Publisher.BBC, Publisher.PA);
        Brand bbcBrand = brand(1, "http://bbc.co.uk/brand", Publisher.BBC);
        List<ItemRef> bbcEpisodes = ImmutableList.of(new ItemRef(Id.valueOf(3), Publisher.BBC, "1", DateTime.now()));
        bbcBrand.setItemRefs(bbcEpisodes);
        
        Brand paBrand = brand(2, "http://pressassociation.com/brand", Publisher.PA);
        List<ItemRef> paEpisodes = ImmutableList.of(new ItemRef(Id.valueOf(4), Publisher.PA, "1", DateTime.now()));
        paBrand.setItemRefs(paEpisodes);
        
        Brand merged = merger.merge(bbcBrand, ImmutableList.of(paBrand), sources);
        assertThat(merged.getItemRefs(), is(equalTo(bbcEpisodes)));
    }
    
    @Test
    public void testContentHierarchyMergingWhenPrecidenceDifferentFromMainPrecedence() {
        ApplicationSources sources = sourcesWithPrecedence(false, Publisher.BBC, Publisher.PA)
                                        .copy()
                                        .withContentHierarchyPrecedence(ImmutableList.of(Publisher.PA, Publisher.BBC))
                                        .build();
        
        Brand bbcBrand = brand(1, "http://bbc.co.uk/brand", Publisher.BBC);
        List<ItemRef> bbcEpisodes = ImmutableList.of(new ItemRef(Id.valueOf(3), Publisher.BBC, "1", DateTime.now()));
        bbcBrand.setItemRefs(bbcEpisodes);
        bbcBrand.setSeriesRefs(ImmutableList.of(new SeriesRef(Id.valueOf(3), Publisher.BBC, "", 1, DateTime.now())));
        
        Brand paBrand = brand(2, "http://pressassociation.com/brand", Publisher.PA);
        List<ItemRef> paEpisodes = ImmutableList.of(new ItemRef(Id.valueOf(4), Publisher.PA, "1", DateTime.now()));
        paBrand.setItemRefs(paEpisodes);
        paBrand.setSeriesRefs(ImmutableList.of(new SeriesRef(Id.valueOf(2), Publisher.PA, "", 2, DateTime.now())));
        
        Brand merged = merger.merge(bbcBrand, ImmutableList.of(paBrand), sources);
        assertThat(merged.getItemRefs(), is(equalTo(paEpisodes)));
        assertThat(merged.getSeriesRefs(), is(equalTo(paBrand.getSeriesRefs())));
    }
    
    @Test
    public void testContentHierarchyMergingWhenPrecidenceSameAsMainPrecedence() {
        ApplicationSources sources = sourcesWithPrecedence(false, Publisher.BBC, Publisher.PA)
                .copy()
                .withContentHierarchyPrecedence(ImmutableList.of(Publisher.BBC, Publisher.PA))
                .build();

        Brand bbcBrand = brand(1, "http://bbc.co.uk/brand", Publisher.BBC);
        List<ItemRef> bbcEpisodes = ImmutableList.of(new ItemRef(Id.valueOf(3), Publisher.BBC, "1", DateTime.now()));
        bbcBrand.setItemRefs(bbcEpisodes);
        
        Brand paBrand = brand(2, "http://pressassociation.com/brand", Publisher.PA);
        List<ItemRef> paEpisodes = ImmutableList.of(new ItemRef(Id.valueOf(4), Publisher.PA, "1", DateTime.now()));
        paBrand.setItemRefs(paEpisodes);
        
        Brand merged = merger.merge(bbcBrand, ImmutableList.of(paBrand), sources);
        assertThat(merged.getItemRefs(), is(equalTo(bbcEpisodes)));   
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

    private ApplicationSources sourcesWithPrecedence(boolean imagePrecedenceEnabled, Publisher...publishers) {
        return ApplicationSources
                .defaults()
                .copy()
                .withPrecedence(true)
                .withImagePrecedenceEnabled(imagePrecedenceEnabled)
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