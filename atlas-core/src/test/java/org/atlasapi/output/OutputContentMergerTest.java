package org.atlasapi.output;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nullable;

import org.atlasapi.application.ApplicationSources;
import org.atlasapi.application.SourceReadEntry;
import org.atlasapi.application.SourceStatus;
import org.atlasapi.content.BlackoutRestriction;
import org.atlasapi.content.Brand;
import org.atlasapi.content.Broadcast;
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
import org.atlasapi.entity.Rating;
import org.atlasapi.entity.Review;
import org.atlasapi.equivalence.EquivalenceRef;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.segment.SegmentEvent;

import com.metabroadcast.common.intl.Countries;
import com.metabroadcast.common.stream.MoreCollectors;

import com.google.common.base.Function;
import com.google.common.collect.Collections2;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.hamcrest.Matchers;
import org.joda.time.DateTime;
import org.joda.time.DateTimeConstants;
import org.joda.time.DateTimeZone;
import org.joda.time.Interval;
import org.junit.Test;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;

public class OutputContentMergerTest {

    //TODO mock hierarchy chooser
    private final OutputContentMerger merger = new OutputContentMerger(new MostPrecidentWithChildrenContentHierarchyChooser());

    @Test
    public void testSortOfCommonSourceContentIsStable() {
        Brand one = brand(1L, "one", Publisher.BBC);
        Brand two = brand(2L, "two", Publisher.BBC);
        Brand three = brand(3L, "three", Publisher.TED);

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
        Brand two = brand(2L, "two", Publisher.PA);
        Brand three = brand(10L, "three", Publisher.TED);

        setEquivalent(one, two, three);
        setEquivalent(two, one, three);
        setEquivalent(three, two, one);

        //two is intentionally missing here
        ImmutableList<Brand> contents = ImmutableList.of(one, three);

        ApplicationSources sources = sourcesWithPrecedence(true, Publisher.BBC, Publisher.TED);
        mergePermutations(contents, sources, one, two.getId());

        one = brand(5L, "one", Publisher.BBC);
        two = brand(2L, "two", Publisher.PA);
        three = brand(10L, "three", Publisher.TED);

        setEquivalent(one, two, three);
        setEquivalent(two, one, three);
        setEquivalent(three, two, one);

        contents = ImmutableList.of(one, three);

        sources = sourcesWithPrecedence(true, Publisher.TED, Publisher.BBC);
        mergePermutations(contents, sources, three, two.getId());
    }

    @Test
    public void testMergeOfAliases() {
        Item one = item(1l, "o", Publisher.METABROADCAST);
        Item two = item(2l, "k", Publisher.BBC);

        one.addAlias(new Alias("a1", "v1"));
        two.addAlias(new Alias("a2", "v2"));

        ApplicationSources sources = sourcesWithPrecedence(
                true,
                Publisher.METABROADCAST,
                Publisher.BBC,
                Publisher.PA
        );
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

        one.setImages(ImmutableList.of(
                Image.builder("test1").build(),
                Image.builder("test2").build()
        ));
        two.setImages(ImmutableList.of(
                Image.builder("test3").build(),
                Image.builder("test4").build()
        ));

        ApplicationSources sources = sourcesWithPrecedence(
                true,
                Publisher.METABROADCAST,
                Publisher.BBC,
                Publisher.PA
        );
        Item merged = merger.merge(one, ImmutableList.of(two, three), sources);

        ImmutableSet<Image> images = merged.getImages().stream()
                .filter(img -> img.getSource() != null)
                .collect(MoreCollectors.toImmutableSet());

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

        one.setImages(ImmutableList.of(
                Image.builder("test1").build(),
                Image.builder("test2").build()
        ));
        two.setImages(ImmutableList.of(
                Image.builder("test3").build(),
                Image.builder("test4").build()
        ));

        ApplicationSources sources = sourcesWithPrecedence(
                false,
                Publisher.METABROADCAST,
                Publisher.BBC,
                Publisher.PA
        );
        Item merged = merger.merge(one, ImmutableList.of(two, three), sources);

        ImmutableSet<Image> images = merged.getImages().stream()
                .filter(img -> img.getSource() != null)
                .collect(MoreCollectors.toImmutableSet());

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

        ApplicationSources sources = sourcesWithPrecedence(
                true,
                Publisher.BBC_KIWI,
                Publisher.BBC_MUSIC
        );

        Item merged = merger.merge(one, ImmutableList.of(two, three), sources);

        List<SegmentEvent> mergedSegmentEvents = merged.getSegmentEvents();

        assertThat(
                mergedSegmentEvents,
                Matchers.is(ImmutableList.of(seOne, seTwo, seFour, seFive))
        );
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

        ApplicationSources sources = sourcesWithPrecedence(
                true,
                Publisher.BBC_KIWI,
                Publisher.METABROADCAST,
                Publisher.BBC_MUSIC
        );

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

        ApplicationSources sources = sourcesWithPrecedence(
                true,
                Publisher.BBC_KIWI,
                Publisher.METABROADCAST,
                Publisher.BBC_MUSIC
        );

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

        ApplicationSources sources = sourcesWithPrecedence(
                true,
                Publisher.BBC_KIWI,
                Publisher.METABROADCAST,
                Publisher.BBC_MUSIC
        );

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

        ApplicationSources sources = sourcesWithPrecedence(
                true,
                Publisher.BBC_KIWI,
                Publisher.METABROADCAST,
                Publisher.BBC_MUSIC
        );

        Container merged = merger.merge(one, ImmutableList.of(two, three), sources);

        assertThat(
                merged.getManifestedAs(),
                is(ImmutableSet.of(encoding1, encoding2, encoding3, encoding4))
        );
    }

    @Test
    public void testImageWithoutMerging() {
        ApplicationSources sources = sourcesWithPrecedence(false, Publisher.BBC, Publisher.PA);
        Item item1 = item(4L, "item1", Publisher.BBC);
        item1.setImages(ImmutableSet.of(new Image("http://image1.org/")));
        Item item2 = item(5L, "item2", Publisher.PA);
        item2.setImages(ImmutableSet.of(new Image("http://image2.org/")));

        Content merged = merger.merge(item1, ImmutableList.of(item2), sources);
        assertThat(merged.getImages().size(), is(2));
    }

    @Test
    public void testImageWithMerging() {
        ApplicationSources sources = sourcesWithPrecedence(true, Publisher.BBC, Publisher.PA);
        Item item1 = item(4L, "item1", Publisher.BBC);
        Image image1 = new Image("http://image1.org/");

        image1.setAvailabilityStart(DateTime.now().minusDays(1));
        image1.setAvailabilityEnd(DateTime.now());
        image1.setType(Image.Type.GENERIC_IMAGE_CONTENT_PLAYER);

        item1.setImages(ImmutableSet.of(image1));

        Item item2 = item(5L, "item2", Publisher.PA);

        Image image2 = new Image("http://image2.org/");

        image1.setAvailabilityStart(DateTime.now().minusDays(1));
        image1.setAvailabilityEnd(DateTime.now());

        item2.setImages(ImmutableSet.of(image2));
        
        Content merged = merger.merge(item2,  ImmutableList.of(item1), sources);
        assertThat(Iterables.getOnlyElement(merged.getImages()).getCanonicalUri(), is("http://image2.org/"));
    }

    @Test
    public void testContentHierarchyMergingWhenNoPrecidenceDefined() {
        ApplicationSources sources = sourcesWithPrecedence(false, Publisher.BBC, Publisher.PA);
        Brand bbcBrand = brand(1, "http://bbc.co.uk/brand", Publisher.BBC);
        List<ItemRef> bbcEpisodes = ImmutableList.of(new ItemRef(
                Id.valueOf(3),
                Publisher.BBC,
                "1",
                DateTime.now()
        ));
        bbcBrand.setItemRefs(bbcEpisodes);

        Brand paBrand = brand(2, "http://pressassociation.com/brand", Publisher.PA);
        List<ItemRef> paEpisodes = ImmutableList.of(new ItemRef(
                Id.valueOf(4),
                Publisher.PA,
                "1",
                DateTime.now()
        ));
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
        List<ItemRef> bbcEpisodes = ImmutableList.of(new ItemRef(
                Id.valueOf(3),
                Publisher.BBC,
                "1",
                DateTime.now()
        ));
        bbcBrand.setItemRefs(bbcEpisodes);
        bbcBrand.setSeriesRefs(ImmutableList.of(new SeriesRef(
                Id.valueOf(3),
                Publisher.BBC,
                "",
                1,
                DateTime.now()
        )));

        Brand paBrand = brand(2, "http://pressassociation.com/brand", Publisher.PA);
        List<ItemRef> paEpisodes = ImmutableList.of(new ItemRef(
                Id.valueOf(4),
                Publisher.PA,
                "1",
                DateTime.now()
        ));
        paBrand.setItemRefs(paEpisodes);
        paBrand.setSeriesRefs(ImmutableList.of(new SeriesRef(
                Id.valueOf(2),
                Publisher.PA,
                "",
                2,
                DateTime.now()
        )));

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
        List<ItemRef> bbcEpisodes = ImmutableList.of(new ItemRef(
                Id.valueOf(3),
                Publisher.BBC,
                "1",
                DateTime.now()
        ));
        bbcBrand.setItemRefs(bbcEpisodes);

        Brand paBrand = brand(2, "http://pressassociation.com/brand", Publisher.PA);
        List<ItemRef> paEpisodes = ImmutableList.of(new ItemRef(
                Id.valueOf(4),
                Publisher.PA,
                "1",
                DateTime.now()
        ));
        paBrand.setItemRefs(paEpisodes);

        Brand merged = merger.merge(bbcBrand, ImmutableList.of(paBrand), sources);
        assertThat(merged.getItemRefs(), is(equalTo(bbcEpisodes)));
    }

    @Test
    // If there are multiple items in the equivalent set belonging to the same source 
    // as the item whose broadcasts are chosen, then broadcasts across all items from
    // that source should be output, since it is assumed that within a data source
    // broadcasts aren't duplicated
    public void testOutputsAllBroadcastsFromPrecedentPublisher() {
        ApplicationSources sources = sourcesWithPrecedence(false, Publisher.BBC, Publisher.PA);

        Item item1 = item(4L, "item1", Publisher.BBC);
        Broadcast b1 = new Broadcast(
                Id.valueOf(1),
                new Interval(
                        new DateTime(2015, DateTimeConstants.JANUARY, 1, 1, 0, 0),
                        new DateTime(2015, DateTimeConstants.JANUARY, 1, 1, 1, 0)
                )
        );
        item1.setBroadcasts(ImmutableSet.of(b1));

        Item item2 = item(5L, "item2", Publisher.PA);
        Broadcast b2 = new Broadcast(
                Id.valueOf(2),
                new Interval(
                        new DateTime(2015, DateTimeConstants.JANUARY, 1, 1, 1, 0),
                        new DateTime(2015, DateTimeConstants.JANUARY, 1, 1, 2, 0)
                )
        );
        item2.setBroadcasts(ImmutableSet.of(b2));

        Item item3 = item(5L, "item3", Publisher.BBC);
        Broadcast b3 = new Broadcast(
                Id.valueOf(3),
                new Interval(
                        new DateTime(2015, DateTimeConstants.JANUARY, 1, 1, 2, 0),
                        new DateTime(2015, DateTimeConstants.JANUARY, 1, 1, 3, 0)
                )
        );
        item3.setBroadcasts(ImmutableSet.of(b3));

        Item merged = merger.merge(item1, ImmutableList.of(item2, item3), sources);
        assertThat(merged.getBroadcasts().size(), is(2));
    }

    @Test
    public void testMergesBroadcastsWithSimilarStartTimes() {

        ApplicationSources sources = sourcesWithPrecedence(false, Publisher.BBC, Publisher.PA)
                .copy()
                .withContentHierarchyPrecedence(ImmutableList.of(Publisher.BBC, Publisher.PA))
                .build();

        DateTime b1StartTime = new DateTime(2015, DateTimeConstants.JANUARY, 1, 1, 20, 0, 0);

        Item item1 = item(4L, "item1", Publisher.BBC);
        Broadcast b1 = new Broadcast(
                Id.valueOf(1),
                new Interval(b1StartTime, b1StartTime.plusMinutes(10))
        );
        b1.addAlias(new Alias("ns1", "v1"));
        item1.setBroadcasts(ImmutableSet.of(b1));

        Item item2 = item(5L, "item2", Publisher.PA);
        DateTime b2StartTime = b1StartTime.plusMinutes(1);

        Broadcast b2 = new Broadcast(
                Id.valueOf(1),
                new Interval(b2StartTime, b2StartTime.plusMinutes(10))
        );
        item2.setBroadcasts(ImmutableSet.of(b2));
        b2.addAlias(new Alias("ns2", "v2"));

        Item merged = merger.merge(item1, ImmutableList.of(item2), sources);
        assertThat(Iterables.getOnlyElement(merged.getBroadcasts()).getAliases().size(), is(2));
    }

    @Test
    public void mergeBroadcastsMergesTheirFields() {
        ApplicationSources sources = sourcesWithPrecedence(false, Publisher.BBC, Publisher.PA)
                .copy()
                .withContentHierarchyPrecedence(ImmutableList.of(Publisher.BBC, Publisher.PA))
                .build();

        DateTime startTime = new DateTime(2015, DateTimeConstants.JANUARY, 1, 1, 20, 0, 0);
        DateTime endTime = new DateTime(2015, DateTimeConstants.JANUARY, 1, 1, 20, 30, 0);

        Broadcast firstItemBroadcast = new Broadcast(
                Id.valueOf(1L),
                new Interval(startTime, endTime)
        );
        firstItemBroadcast.setLive(false);

        Broadcast secondItemBroadcast = new Broadcast(
                Id.valueOf(1L),
                new Interval(startTime, endTime)
        );
        secondItemBroadcast.setNewEpisode(true);

        Item firstItem = item(10L, "uriFirst", Publisher.BBC);
        Item secondItem = item(11L, "uriSecond", Publisher.PA);

        firstItem.addBroadcast(firstItemBroadcast);
        secondItem.addBroadcast(secondItemBroadcast);


        Item merged = merger.merge(firstItem, ImmutableList.of(secondItem), sources);

        Broadcast actualBroadcast = Iterables.getOnlyElement(merged.getBroadcasts());
        assertThat(
                actualBroadcast.getNewEpisode(),
                is(secondItemBroadcast.getNewEpisode())
        );
        assertThat(
                actualBroadcast.getLive(),
                is(firstItemBroadcast.getLive())
        );
    }

    @Test
    public void mergeAllReviewsToChosenContent() {
        // content belonging to sources that the user does not have permission to access
        // have already been whittled out

        ApplicationSources sources = sourcesWithPrecedence(false, Publisher.PA, Publisher.BBC, Publisher.RADIO_TIMES).copy().build();

        Item chosenItem = item(10L, "uriFirst", Publisher.RADIO_TIMES);
        Item firstEquivItem = item(11L, "uriSecond", Publisher.PA);
        Item secondEquivItem = item(12L, "uriSecond", Publisher.BBC);

        Set<Review> expectedReviews = new ImmutableSet.Builder<Review>()
                .addAll(addReviews(chosenItem, "review 1", "review 2"))
                .addAll(addReviews(firstEquivItem, "review 3", "review 4"))
                .addAll(addReviews(secondEquivItem, "review 5"))
                .build();

        // chosenItem is mutated by merge, so calculate this first
        int expectedReviewsCount = expectedReviews.size();

        Item merged = merger.merge(chosenItem, ImmutableList.of(firstEquivItem, secondEquivItem), sources);

        assertThat(merged.getReviews().size(), is(expectedReviewsCount));
        assertThat(merged.getReviews().containsAll(expectedReviews), is(true));
    }

    private List<Review> addReviews(Item item, String... reviewComments) {
        Optional<Publisher> publisher = Optional.of(item.getSource());
        List<Review> reviews = Arrays.asList(reviewComments).stream()
                .map(comment -> new Review(null, comment, publisher))
                .collect(Collectors.toList());

        item.setReviews(ImmutableList.copyOf(reviews));
        return reviews;
    }

    @Test
    public void mergeAllRatingsToChosenContent() {
        // content belonging to sources that the user does not have permission to access
        // have already been whittled out

        ApplicationSources sources = sourcesWithPrecedence(false, Publisher.PA, Publisher.BBC, Publisher.RADIO_TIMES).copy().build();

        Item chosenItem = item(10L, "uriFirst", Publisher.RADIO_TIMES);
        Item firstEquivItem = item(11L, "uriSecond", Publisher.PA);
        Item secondEquivItem = item(12L, "uriSecond", Publisher.BBC);

        Set<Rating> expectedRatings = new ImmutableSet.Builder<Rating>()
                .addAll(addRatings(chosenItem, 0.1f, 0.2f))
                .addAll(addRatings(firstEquivItem, 0.3f, 0.4f))
                .addAll(addRatings(secondEquivItem, 0.5f))
                .build();

        // chosenItem is mutated by merge, so calculate this first
        int expectedRatingsCount = expectedRatings.size();

        Item merged = merger.merge(chosenItem, ImmutableList.of(firstEquivItem, secondEquivItem), sources);

        assertThat(merged.getRatings().size(), is(expectedRatingsCount));
        assertThat(merged.getRatings().containsAll(expectedRatings), is(true));
    }

    @Test
    public void mergeBlackoutReturnsBlackoutTrueIfChosenNoBlackoutAndNonChosenBlackoutTrue() {
        com.google.common.base.Optional<BlackoutRestriction> restriction = 
                getMergedBlackoutRestriction(
                        null,
                        new BlackoutRestriction(true)
        );

        assertThat(restriction.isPresent(), is(true));
        assertThat(restriction.get().getAll(), is(true));
    }

    @Test
    public void mergeBlackoutReturnsBlackoutFalseIfChosenNoBlackoutAndNonChosenBlackoutFalse() {
        com.google.common.base.Optional<BlackoutRestriction> restriction = 
                getMergedBlackoutRestriction(
                null,
                new BlackoutRestriction(false)
        );

        assertThat(restriction.isPresent(), is(true));
        assertThat(restriction.get().getAll(), is(false));
    }

    @Test
    public void mergeBlackoutReturnsBlackoutTrueIfChosenBlackoutFalseAndNonChosenBlackoutTrue() {
        com.google.common.base.Optional<BlackoutRestriction> restriction = 
                getMergedBlackoutRestriction(
                        new BlackoutRestriction(false),
                        new BlackoutRestriction(true)
        );

        assertThat(restriction.isPresent(), is(true));
        assertThat(restriction.get().getAll(), is(true));
    }

    @Test
    public void mergeBlackoutReturnsBlackoutTrueIfChosenBlackoutTrueAndNonChosenBlackoutFalse() {
        com.google.common.base.Optional<BlackoutRestriction> restriction = 
                getMergedBlackoutRestriction(
                        new BlackoutRestriction(true),
                        new BlackoutRestriction(false)
        );

        assertThat(restriction.isPresent(), is(true));
        assertThat(restriction.get().getAll(), is(true));
    }

    @Test
    public void mergeBlackoutReturnsBlackoutFalseIfChosenBlackoutFalseAndNonChosenBlackoutFalse() {
        com.google.common.base.Optional<BlackoutRestriction> restriction =
                getMergedBlackoutRestriction(
                        new BlackoutRestriction(false),
                        new BlackoutRestriction(false)
                );

        assertThat(restriction.isPresent(), is(true));
        assertThat(restriction.get().getAll(), is(false));
    }

    @Test
    public void mergeBlackoutReturnsNoBlackoutIfChosenNoBlackoutAndNonChosenNoBlackout() {
        com.google.common.base.Optional<BlackoutRestriction> restriction =
                getMergedBlackoutRestriction(
                        null,
                        null
                );

        assertThat(restriction.isPresent(), is(false));
    }

    private List<Rating> addRatings(Item item, Float... ratingValues) {
        Publisher publisher = item.getSource();
        List<Rating> ratings = Arrays.asList(ratingValues).stream()
                .map(rating -> new Rating("5STAR", rating, publisher))
                .collect(Collectors.toList());

        item.setRatings(ratings);
        return ratings;
    }

    private Brand brand(long id, String uri, Publisher source) {
        Brand one = new Brand(uri, uri, source);
        one.setId(id);
        return one;
    }

    private Item item(long id, String uri, Publisher source) {
        Item one = new Item(uri, uri, source);
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

    private ApplicationSources sourcesWithPrecedence(boolean imagePrecedenceEnabled,
            Publisher... publishers) {
        return ApplicationSources
                .defaults()
                .copy()
                .withPrecedence(true)
                .withImagePrecedenceEnabled(imagePrecedenceEnabled)
                .withReadableSources(Lists.transform(
                        ImmutableList.copyOf(publishers),
                        new Function<Publisher, SourceReadEntry>() {

                            @Override
                            public SourceReadEntry apply(Publisher input) {
                                return new SourceReadEntry(input, SourceStatus.AVAILABLE_ENABLED);
                            }
                        }
                ))
                .build();
    }

    private void setEquivalent(Content receiver, Content... equivalents) {
        ImmutableList<Content> allContent = ImmutableList.<Content>builder()
                .add(receiver)
                .addAll(ImmutableList.copyOf(equivalents))
                .build();
        receiver.setEquivalentTo(ImmutableSet.copyOf(Iterables.transform(
                allContent, EquivalenceRef.toEquivalenceRef())
        ));
    }

    @SuppressWarnings("Guava")
    private com.google.common.base.Optional<BlackoutRestriction> getMergedBlackoutRestriction(
            @Nullable BlackoutRestriction chosenRestriction,
            @Nullable BlackoutRestriction toMergeRestriction
    ) {
        Item chosen = item(1L, "o", Publisher.METABROADCAST);
        Item notChosen = item(2L, "k", Publisher.BBC);

        Id channelId = Id.valueOf(0L);
        DateTime start = DateTime.now();
        DateTime end = DateTime.now().plusHours(1);

        Broadcast chosenBroadcast = new Broadcast(channelId, start, end);
        chosenBroadcast.setBlackoutRestriction(chosenRestriction);

        Broadcast notChosenBroadcast = new Broadcast(channelId, start, end);
        notChosenBroadcast.setBlackoutRestriction(toMergeRestriction);

        chosen.addBroadcast(chosenBroadcast);
        notChosen.addBroadcast(notChosenBroadcast);

        ApplicationSources sources = sourcesWithPrecedence(
                true,
                Publisher.METABROADCAST,
                Publisher.BBC
        );

        Item merged = merger.merge(chosen, ImmutableList.of(notChosen), sources);

        return Iterables
                .getOnlyElement(merged.getBroadcasts())
                .getBlackoutRestriction();
    }
}
