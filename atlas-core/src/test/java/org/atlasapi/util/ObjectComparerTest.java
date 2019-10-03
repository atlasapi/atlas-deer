package org.atlasapi.util;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Multimap;
import com.metabroadcast.common.currency.Price;
import com.metabroadcast.common.intl.Countries;
import com.metabroadcast.common.media.MimeType;
import org.atlasapi.comparison.ExcludeFromObjectComparison;
import org.atlasapi.comparison.ObjectComparer;
import org.atlasapi.content.BlackoutRestriction;
import org.atlasapi.content.Brand;
import org.atlasapi.content.BrandRef;
import org.atlasapi.content.Broadcast;
import org.atlasapi.content.BroadcastRef;
import org.atlasapi.content.Certificate;
import org.atlasapi.content.Clip;
import org.atlasapi.content.Container;
import org.atlasapi.content.ContainerSummary;
import org.atlasapi.content.Content;
import org.atlasapi.content.ContentGroupRef;
import org.atlasapi.content.CrewMember;
import org.atlasapi.content.Described;
import org.atlasapi.content.Description;
import org.atlasapi.content.Encoding;
import org.atlasapi.content.Episode;
import org.atlasapi.content.Film;
import org.atlasapi.content.Image;
import org.atlasapi.content.Item;
import org.atlasapi.content.ItemRef;
import org.atlasapi.content.ItemSummary;
import org.atlasapi.content.KeyPhrase;
import org.atlasapi.content.Location;
import org.atlasapi.content.LocationSummary;
import org.atlasapi.content.MediaType;
import org.atlasapi.content.Policy;
import org.atlasapi.content.Pricing;
import org.atlasapi.content.Priority;
import org.atlasapi.content.PriorityScoreReasons;
import org.atlasapi.content.Quality;
import org.atlasapi.content.RelatedLink;
import org.atlasapi.content.ReleaseDate;
import org.atlasapi.content.Restriction;
import org.atlasapi.content.Series;
import org.atlasapi.content.SeriesRef;
import org.atlasapi.content.Song;
import org.atlasapi.content.Specialization;
import org.atlasapi.content.Subtitles;
import org.atlasapi.content.Synopses;
import org.atlasapi.content.Tag;
import org.atlasapi.content.TransportSubType;
import org.atlasapi.content.TransportType;
import org.atlasapi.entity.Alias;
import org.atlasapi.entity.Award;
import org.atlasapi.entity.Id;
import org.atlasapi.entity.Identified;
import org.atlasapi.entity.Rating;
import org.atlasapi.entity.Review;
import org.atlasapi.equivalence.EquivalenceRef;
import org.atlasapi.event.EventRef;
import org.atlasapi.hashing.ExcludeFromHash;
import org.atlasapi.hashing.Hashable;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.segment.SegmentEvent;
import org.atlasapi.segment.SegmentRef;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.joda.time.Interval;
import org.joda.time.LocalDate;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Currency;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class ObjectComparerTest {

    private ObjectComparer comparer;

    @Before
    public void setUp() throws Exception {
        comparer = new ObjectComparer();
    }

    @Test
    public void testComparerIgnoresExcludedFields() {
        ExcludedFieldsTestClass o1 = new ExcludedFieldsTestClass(1, 10);
        ExcludedFieldsTestClass o2 = new ExcludedFieldsTestClass(1, 20);
        ExcludedFieldsTestClass o3 = new ExcludedFieldsTestClass(2, 10);
        assertTrue(comparer.equals(o1, o2));
        assertFalse(comparer.equals(o1, o3));

        DelegateIsExcludedFieldsClass o4 = new DelegateIsExcludedFieldsClass(o1, 1);
        DelegateIsExcludedFieldsClass o5 = new DelegateIsExcludedFieldsClass(o2, 1);
        DelegateIsExcludedFieldsClass o6 = new DelegateIsExcludedFieldsClass(o1, 2);
        assertTrue(comparer.equals(o4, o5));
        assertFalse(comparer.equals(o4, o6));
    }

    @Test
    public void testComparerIgnoresExactClassType() {
        SetTestClass immutableSetBackedObject = new SetTestClass(ImmutableSet.of(1));
        SetTestClass immutableSetBackedObject2 = new SetTestClass(ImmutableSet.of(2));
        HashSet<Integer> hashSet1 = new HashSet<>();
        HashSet<Integer> hashSet2 = new HashSet<>();
        hashSet1.add(1);
        hashSet2.add(2);
        SetTestClass hashSetBackedObject = new SetTestClass(hashSet1);
        SetTestClass hashSetBackedObject2 = new SetTestClass(hashSet2);
        assertTrue(comparer.equals(immutableSetBackedObject, hashSetBackedObject));
        assertTrue(comparer.equals(immutableSetBackedObject2, hashSetBackedObject2));
        assertFalse(comparer.equals(immutableSetBackedObject, immutableSetBackedObject2));
        assertFalse(comparer.equals(hashSetBackedObject, hashSetBackedObject2));
    }

    @Test
    public void testComparerIgnoresOverridenEqualsFields() {
        FieldIgnoringEqualsClass o1 = new FieldIgnoringEqualsClass(1, 10);
        FieldIgnoringEqualsClass o2 = new FieldIgnoringEqualsClass(1, 20);
        FieldIgnoringEqualsClass o3 = new FieldIgnoringEqualsClass(1, 10);
        assertEquals(o1, o2);
        assertFalse(comparer.equals(o1, o2));
        assertTrue(comparer.equals(o1, o3));

        FieldsIgnoringEqualsClass o4 = new FieldsIgnoringEqualsClass(1, 10, 20, 30);
        FieldsIgnoringEqualsClass o5 = new FieldsIgnoringEqualsClass(1, 10, 20, 35);
        FieldsIgnoringEqualsClass o6 = new FieldsIgnoringEqualsClass(1, 15, 20, 30);
        FieldsIgnoringEqualsClass o7 = new FieldsIgnoringEqualsClass(1, 10, 20, 30);
        assertEquals(o4, o5);
        assertEquals(o4, o6);
        assertFalse(comparer.equals(o4, o5));
        assertFalse(comparer.equals(o4, o6));
        assertTrue(comparer.equals(o4, o7));
    }

    @Test
    public void testEqualOverrideOfSeriesRefIsIgnored() {
        Brand b1 = new Brand();
        Brand b2 = new Brand();
        b1.setId(10);
        b2.setId(10);
        DateTime now = DateTime.now();
        b1.setLastUpdated(now);
        b2.setLastUpdated(now.plusHours(1));
        assertTrue(comparer.equals(b1, b2));
        SeriesRef seriesRef1 = new SeriesRef(Id.valueOf(1), Publisher.METABROADCAST, "title1", 1, now);
        SeriesRef seriesRef2 = new SeriesRef(Id.valueOf(1), Publisher.METABROADCAST, "title2", 1, now);
        Set<SeriesRef> seriesRefs1 = ImmutableSet.of(seriesRef1);
        Set<SeriesRef> seriesRefs2 = new HashSet<>();
        seriesRefs2.add(seriesRef2);
        b1.setSeriesRefs(seriesRefs1);
        b2.setSeriesRefs(seriesRefs2);
        assertFalse(comparer.equals(b1, b2));
    }

    @Test
    public void getValueIsSameForTwoItemsWithSameFields() throws Exception {
        Item content = new Item();
        Item content2 = new Item();

        DateTime now = DateTime.now();

        setIdentifiedFields(content, now);
        setIdentifiedFields(content2, now);
        setDescribedFields(content, now);
        setDescribedFields(content2, now);
        setContentFields(content, true, now);
        setContentFields(content2, true, now);
        setItemFields(content, now);
        setItemFields(content2, now);

        verifyAllFieldsAreSet(content, "Item");
        verifyAllFieldsAreSet(content2, "Item");

        assertTrue(comparer.equals(content, content2));
    }

    @Test
    public void getValueIgnoresDatabaseSpecificDateFields() throws Exception {
        Item content = new Item();
        Item content2 = new Item();

        DateTime now = DateTime.now();

        setIdentifiedFields(content, now);
        setIdentifiedFields(content2, now);
        setDescribedFields(content, now);
        setDescribedFields(content2, now);
        setContentFields(content, true, now);
        setContentFields(content2, true, now);
        setItemFields(content, now);
        setItemFields(content2, now);

        content.setLastUpdated(now);
        content2.setLastUpdated(now.plusHours(1));
        content.setThisOrChildLastUpdated(now);
        content2.setThisOrChildLastUpdated(now.plusHours(1));
        content.setFirstSeen(now);
        content2.setFirstSeen(now.plusHours(1));

        verifyAllFieldsAreSet(content, "Item");
        verifyAllFieldsAreSet(content2, "Item");

        assertTrue(comparer.equals(content, content2));
    }

    @Test
    public void getValueIsSameForTwoSongsWithSameFields() throws Exception {
        Song content = new Song();
        Song content2 = new Song();

        DateTime now = DateTime.now();

        setIdentifiedFields(content, now);
        setIdentifiedFields(content2, now);
        setDescribedFields(content, now);
        setDescribedFields(content2, now);
        setContentFields(content, true, now);
        setContentFields(content2, true, now);
        setItemFields(content, now);
        setItemFields(content2, now);
        setSongFields(content);
        setSongFields(content2);

        verifyAllFieldsAreSet(content, "Song");
        verifyAllFieldsAreSet(content2, "Song");

        assertTrue(comparer.equals(content, content2));
    }

    @Test
    public void getValueIsSameForTwoClipsWithSameFields() throws Exception {
        Clip content = new Clip();
        Clip content2 = new Clip();

        DateTime now = DateTime.now();

        setIdentifiedFields(content, now);
        setIdentifiedFields(content2, now);
        setDescribedFields(content, now);
        setDescribedFields(content2, now);
        setContentFields(content, true, now);
        setContentFields(content2, true, now);
        setItemFields(content, now);
        setItemFields(content2, now);
        setClipFields(content);
        setClipFields(content2);

        verifyAllFieldsAreSet(content, "Clip");
        verifyAllFieldsAreSet(content2, "Clip");

        assertTrue(comparer.equals(content, content2));
    }

    @Test
    public void getValueIsSameForTwoEpisodesWithSameFields() throws Exception {
        Episode content = new Episode();
        Episode content2 = new Episode();

        DateTime now = DateTime.now();

        setIdentifiedFields(content, now);
        setIdentifiedFields(content2, now);
        setDescribedFields(content, now);
        setDescribedFields(content2, now);
        setContentFields(content, true, now);
        setContentFields(content2, true, now);
        setItemFields(content, now);
        setItemFields(content2, now);
        setEpisodeFields(content, now);
        setEpisodeFields(content2, now);

        verifyAllFieldsAreSet(content, "Episode");
        verifyAllFieldsAreSet(content2, "Episode");

        assertTrue(comparer.equals(content, content2));
    }

    @Test
    public void getValueIsSameForTwoFilmsWithSameFields() throws Exception {
        Film content = new Film();
        Film content2 = new Film();

        DateTime now = DateTime.now();

        setIdentifiedFields(content, now);
        setIdentifiedFields(content2, now);
        setDescribedFields(content, now);
        setDescribedFields(content2, now);
        setContentFields(content, true, now);
        setContentFields(content2, true, now);
        setItemFields(content, now);
        setItemFields(content2, now);
        setFilmFields(content);
        setFilmFields(content2);

        verifyAllFieldsAreSet(content, "Film");
        verifyAllFieldsAreSet(content2, "Film");

        assertTrue(comparer.equals(content, content2));
    }

    @Test
    public void getValueIsSameForTwoBrandsWithSameFields() throws Exception {
        Brand content = new Brand();
        Brand content2 = new Brand();

        DateTime now = DateTime.now();

        setIdentifiedFields(content, now);
        setIdentifiedFields(content2, now);
        setDescribedFields(content, now);
        setDescribedFields(content2, now);
        setContentFields(content, true, now);
        setContentFields(content2, true, now);
        setContainerFields(content, now);
        setContainerFields(content2, now);
        setBrandFields(content, now);
        setBrandFields(content2, now);

        verifyAllFieldsAreSet(content, "Brand");
        verifyAllFieldsAreSet(content2, "Brand");

        assertTrue(comparer.equals(content, content2));
    }

    @Test
    public void getValueIsSameForTwoSeriesWithSameFields() throws Exception {
        Series content = new Series();
        Series content2 = new Series();

        DateTime now = DateTime.now();

        setIdentifiedFields(content, now);
        setIdentifiedFields(content2, now);
        setDescribedFields(content, now);
        setDescribedFields(content2, now);
        setContentFields(content, true, now);
        setContentFields(content2, true, now);
        setContainerFields(content, now);
        setContainerFields(content2, now);
        setSeriesFields(content);
        setSeriesFields(content2);

        verifyAllFieldsAreSet(content, "Series");
        verifyAllFieldsAreSet(content2, "Series");

        assertTrue(comparer.equals(content, content2));
    }

    private void setIdentifiedFields(Identified identified) {
        setIdentifiedFields(identified, DateTime.now());
    }

    private void setIdentifiedFields(Identified identified, DateTime timestamp) {
        identified.setId(Id.valueOf(1L));
        identified.setCanonicalUri("canonicalUri");
        identified.setCurie("curie");
        identified.setAliasUrls(ImmutableSet.of("alias"));
        identified.setAliases(ImmutableSet.of(new Alias("ns", "value")));
        identified.setEquivalentTo(ImmutableSet.of(
                new EquivalenceRef(Id.valueOf(2L), Publisher.METABROADCAST))
        );
        identified.setLastUpdated(timestamp);
        identified.setEquivalenceUpdate(timestamp);
        identified.addCustomField("testField", "testValue");
    }

    private void setDescribedFields(Described described) {
        setDescribedFields(described, DateTime.now());
    }

    private void setDescribedFields(Described described, DateTime timestamp) {
        described.setTitle("title");
        described.setShortDescription("short");
        described.setMediumDescription("medium");
        described.setLongDescription("long");

        Synopses synopses = new Synopses();
        synopses.setShortDescription("short");
        synopses.setMediumDescription("medium");
        synopses.setLongDescription("long");
        described.setSynopses(synopses);

        described.setDescription("description");
        described.setMediaType(MediaType.AUDIO);
        described.setSpecialization(Specialization.FILM);
        described.setGenres(ImmutableSet.of("genre"));
        described.setPublisher(Publisher.METABROADCAST);
        described.setImage("image");

        Image image = Image.builder("uri")
                .withType(Image.Type.LOGO)
                .withColor(Image.Color.BLACK_AND_WHITE)
                .withTheme(Image.Theme.DARK_OPAQUE)
                .withHeight(50)
                .withWidth(50)
                .withAspectRatio(Image.AspectRatio.SIXTEEN_BY_NINE)
                .withMimeType(MimeType.IMAGE_PNG)
                .withAvailabilityStart(timestamp)
                .withAvailabilityEnd(timestamp)
                .withHasTitleArt(true)
                .withSource(Publisher.METABROADCAST)
                .build();
        described.setImages(ImmutableSet.of(image));

        described.setThumbnail("thumbnail");
        described.setFirstSeen(timestamp);
        described.setLastFetched(timestamp);
        described.setThisOrChildLastUpdated(timestamp);
        described.setScheduleOnly(false);
        described.setActivelyPublished(true);
        described.setPresentationChannel("channel");

        Priority priority = new Priority();
        priority.setReasons(new PriorityScoreReasons(
                ImmutableList.of("positive"),
                ImmutableList.of("negative")
        ));
        priority.setPriority(1.0);
        described.setPriority(priority);

        RelatedLink relatedLink = RelatedLink.relatedLink(RelatedLink.LinkType.UNKNOWN, "uri")
                .withSourceId("sourceId")
                .withShortName("short")
                .withTitle("title")
                .withDescription("description")
                .withImage("image")
                .withThumbnail("thumbnail")
                .build();
        described.setRelatedLinks(ImmutableSet.of(relatedLink));

        described.setReviews(ImmutableSet.of(
                Review.builder("review")
                        .withLocale(Locale.ENGLISH)
                        .withSource(Optional.of(Publisher.METABROADCAST))
                        .build()
        ));
        described.setRatings(ImmutableSet.of(
                new Rating("type", 5F, Publisher.METABROADCAST)
        ));

        Award award = new Award();
        award.setOutcome("outcome");
        award.setTitle("title");
        award.setDescription("description");
        award.setYear(2000);
        described.setAwards(ImmutableSet.of(award));
    }

    private void setContentFields(Content content, boolean addClips) {
        setContentFields(content, addClips, DateTime.now());
    }

    private void setContentFields(Content content, boolean addClips, DateTime timestamp) {
        if (addClips) {
            Clip clip = new Clip();

            setIdentifiedFields(clip, timestamp);
            setDescribedFields(clip, timestamp);
            setContentFields(clip, false, timestamp);
            setItemFields(clip, timestamp);

            clip.setClipOf("clipOf");

            content.setClips(ImmutableSet.of(clip));
        }

        content.setKeyPhrases(ImmutableSet.of(
                new KeyPhrase("phrase", 1.0)
        ));
        content.setTags(ImmutableSet.of(
                Tag.valueOf(Id.valueOf(0L), 1.0F, false, Tag.Relationship.ABOUT)
        ));
        content.setContentGroupRefs(ImmutableSet.of(
                new ContentGroupRef(Id.valueOf(1L), "uri")
        ));
        content.setPeople(ImmutableList.of(
                new CrewMember("uri", "curie", Publisher.METABROADCAST)
                        .withRole(CrewMember.Role.ACTOR)
                        .withName("name")
        ));
        content.setLanguages(ImmutableSet.of("english"));
        content.setCertificates(ImmutableSet.of(
                new Certificate("classification", Countries.GB)
        ));
        content.setYear(2000);
        content.setGenericDescription(false);
        content.setEventRefs(ImmutableSet.of(
                new EventRef(Id.valueOf(1L), Publisher.METABROADCAST)
        ));
        content.setCountriesOfOrigin(ImmutableSet.of(Countries.GB));

        Encoding encoding = new Encoding();

        setIdentifiedFields(encoding, timestamp);
        encoding.setContainsAdvertising(true);
        encoding.setAdvertisingDuration(50);
        encoding.setDuration(Duration.standardHours(1));
        encoding.setBitRate(1);
        encoding.setAudioBitRate(50);
        encoding.setAudioChannels(1);
        encoding.setAudioCoding(MimeType.AUDIO_MP3);
        encoding.setVideoAspectRatio("16:9");
        encoding.setVideoBitRate(10);
        encoding.setVideoCoding(MimeType.VIDEO_MP4);
        encoding.setVideoFrameRate(10F);
        encoding.setVideoHorizontalSize(1000);
        encoding.setVideoProgressiveScan(true);
        encoding.setVideoVerticalSize(500);
        encoding.setDataSize(10L);
        encoding.setDataContainerFormat(MimeType.APPLICATION_JSON);
        encoding.setSource("source");
        encoding.setDistributor("distributor");
        encoding.setHasDOG(false);
        encoding.set3d(false);
        encoding.setQuality(Quality.FOUR_K);
        encoding.setQualityDetail("detail");
        encoding.setVersionId("5");

        Location location = new Location();

        setIdentifiedFields(location, timestamp);
        location.setAvailable(true);
        location.setTransportIsLive(false);
        location.setTransportSubType(TransportSubType.HTTP);
        location.setTransportType(TransportType.DOWNLOAD);
        location.setUri("uri");
        location.setEmbedCode("embed");

        Policy policy = new Policy();

        setIdentifiedFields(policy, timestamp);
        policy.setAvailabilityStart(timestamp);
        policy.setAvailabilityEnd(timestamp);
        policy.setDrmPlayableFrom(timestamp);
        policy.setAvailableCountries(ImmutableSet.of(Countries.GB));
        policy.setAvailabilityLength(10);
        policy.setRevenueContract(Policy.RevenueContract.FREE_TO_VIEW);
        policy.setSubscriptionPackages(ImmutableSet.of("subscription"));
        policy.setPrice(new Price(Currency.getInstance("GBP"), 10));
        policy.setPricing(ImmutableList.of(new Pricing(
                timestamp, timestamp, new Price(Currency.getInstance("GBP"), 10)
        )));
        policy.setServiceId(Id.valueOf(10L));
        policy.setPlayerId(Id.valueOf(11L));
        policy.setPlatform(Policy.Platform.BTVISION_CARDINAL);
        policy.setNetwork(Policy.Network.THREE_G);
        policy.setActualAvailabilityStart(timestamp);

        location.setPolicy(policy);

        encoding.setAvailableAt(ImmutableSet.of(location));

        content.setManifestedAs(ImmutableSet.of(encoding));
    }

    private void setItemFields(Item item) {
        setItemFields(item, DateTime.now());
    }

    private void setItemFields(Item item, DateTime timestamp) {
        item.setContainerRef(new BrandRef(Id.valueOf(10L), Publisher.METABROADCAST));
        item.setIsLongForm(true);
        item.setBlackAndWhite(false);
        item.setContainerSummary(ContainerSummary.create(
                "type", "title", "description", 5, 5
        ));

        Broadcast broadcast = new Broadcast(
                Id.valueOf(10L), timestamp, timestamp, true
        );
        setIdentifiedFields(broadcast, timestamp);

        broadcast.setScheduleDate(LocalDate.now());
        broadcast.withId("sourceId");
        broadcast.setVersionId("versionId");
        broadcast.setRepeat(false);
        broadcast.setRevisedRepeat(false);
        broadcast.setSubtitled(true);
        broadcast.setSigned(false);
        broadcast.setAudioDescribed(false);
        broadcast.setHighDefinition(true);
        broadcast.setWidescreen(true);
        broadcast.setSurround(false);
        broadcast.setLive(true);
        broadcast.setNewSeries(true);
        broadcast.setNewEpisode(false);
        broadcast.setNewOneOff(true);
        broadcast.setPremiere(false);
        broadcast.setContinuation(false);
        broadcast.set3d(false);
        broadcast.setBlackoutRestriction(new BlackoutRestriction(true));

        item.setBroadcasts(ImmutableSet.of(broadcast));

        SegmentEvent segmentEvent = new SegmentEvent();

        segmentEvent.setPosition(1);
        segmentEvent.setOffset(Duration.ZERO);
        segmentEvent.setIsChapter(false);
        segmentEvent.setDescription(new Description("title", "synopsis", "image", "thumbnail"));
        segmentEvent.setSegment(new SegmentRef(Id.valueOf(10L), Publisher.METABROADCAST));
        segmentEvent.setVersionId("5");
        segmentEvent.setPublisher(Publisher.METABROADCAST);

        setIdentifiedFields(segmentEvent, timestamp);

        item.setSegmentEvents(ImmutableSet.of(segmentEvent));

        Restriction restriction = new Restriction();

        restriction.setRestricted(true);
        restriction.setMinimumAge(5);
        restriction.setMessage("message");
        restriction.setAuthority("authority");
        restriction.setRating("rating");

        setIdentifiedFields(restriction, timestamp);

        item.setRestrictions(ImmutableSet.of(restriction));
    }

    private void setSongFields(Song song) {
        song.setIsrc("isrc");
        song.setDuration(Duration.standardMinutes(1));
    }

    private void setClipFields(Clip clip) {
        clip.setClipOf("clipOf");
    }

    private void setFilmFields(Film film) {
        film.setWebsiteUrl("website");
        film.setSubtitles(ImmutableSet.of(
                new Subtitles("EN")
        ));
        film.setReleaseDates(ImmutableSet.of(
                new ReleaseDate(LocalDate.now(), Countries.GB, ReleaseDate.ReleaseType.GENERAL)
        ));
    }

    private void setEpisodeFields(Episode episode) {
        setEpisodeFields(episode, DateTime.now());
    }

    private void setEpisodeFields(Episode episode, DateTime timestamp) {
        episode.setSeriesNumber(5);
        episode.setEpisodeNumber(5);
        episode.setPartNumber(1);
        episode.setSpecial(false);
        episode.setSeriesRef(new SeriesRef(
                Id.valueOf(0L),
                Publisher.METABROADCAST,
                "title",
                5,
                timestamp,
                2000,
                ImmutableSet.of(new Certificate("18", Countries.GB))
        ));
    }

    private void setContainerFields(Container container) {
        setContainerFields(container, DateTime.now());
    }

    private void setContainerFields(Container container, DateTime timestamp) {
        ItemRef itemRef = new ItemRef(
                Id.valueOf(10L),
                Publisher.METABROADCAST,
                "sort",
                timestamp
        );
        container.setItemRefs(ImmutableSet.of(itemRef));
        container.setUpcomingContent(ImmutableMap.of(
                itemRef,
                ImmutableSet.of(
                        new BroadcastRef(
                                "id",
                                Id.valueOf(1L),
                                new Interval(timestamp, timestamp.plusDays(1))
                        )
                )
        ));
        container.setAvailableContent(ImmutableMap.of(
                itemRef,
                ImmutableSet.of(
                        new LocationSummary(
                                true,
                                "uri",
                                timestamp.minusDays(1),
                                timestamp.plusDays(1)
                        )
                )
        ));
        container.setItemSummaries(ImmutableList.of(new ItemSummary(
                itemRef,
                "title",
                "description",
                "image",
                2000,
                ImmutableSet.of(new Certificate("18", Countries.GB))
        )));
    }

    private void setBrandFields(Brand brand) {
        setBrandFields(brand, DateTime.now());
    }

    private void setBrandFields(Brand brand, DateTime timestamp) {
        brand.setSeriesRefs(ImmutableSet.of(new SeriesRef(
                Id.valueOf(1L),
                Publisher.METABROADCAST,
                "title",
                1,
                timestamp,
                2000,
                ImmutableSet.of(new Certificate("18", Countries.GB))
        )));
    }

    private void setSeriesFields(Series series) {
        series.withSeriesNumber(1);
        series.setTotalEpisodes(1);
        series.setBrandRef(new BrandRef(Id.valueOf(1L), Publisher.METABROADCAST));
    }

    // Ensure the test has set a value for all fields of the tested object. This is to ensure
    // that we catch new fields that may have been added to the tested object so as to cover
    // them with tests here.
    @SuppressWarnings("ThrowFromFinallyBlock")
    private void verifyAllFieldsAreSet(Object object, String path) throws Exception {
        if (object == null) {
            failWithUnsetField(path);

        } else if (object instanceof Iterable) {
            Iterable<?> iterable = (Iterable<?>) object;
            if (Iterables.isEmpty(iterable)) {
                failWithUnsetField(path);
            }
            int idx = 0;
            for (Object o : iterable) {
                verifyAllFieldsAreSet(o, path + " [" + idx + "]");
            }
        } else if (object.getClass().isArray()) {
            for (int i = 0; i < Array.getLength(object); i++) {
                Object arrayObject = Array.get(object, i);
                verifyAllFieldsAreSet(arrayObject, path + " [" + i + "]");
            }

        } else if (object instanceof Map) {
            verifyAllFieldsAreSet(((Map<?, ?>) object).entrySet(), path + " -> .entrySet()");
        } else if (object instanceof Multimap) {
            verifyAllFieldsAreSet(((Multimap<?, ?>) object).entries(), path + " -> .entries()");
        } else if (object instanceof Map.Entry) {
            Map.Entry<?, ?> e = (Map.Entry<?, ?>) object;
            verifyAllFieldsAreSet(e.getKey(), path + "<key>");
            verifyAllFieldsAreSet(e.getValue(), path + "<val>[" + e.getKey() + "]");

        } else if (object instanceof Hashable) {
            for (Field field : getFields(object)) {
                if (Clip.class.isAssignableFrom(field.getType())
                        || field.getName().equals("clips")) {
                    // Clips are items which contain clips... This is to avoid an infinite loop
                    // of checking if all fields are populated
                    return;
                }

                if (Modifier.isStatic(field.getModifiers())) {
                    continue;
                }
                if (field.isAnnotationPresent(ExcludeFromHash.class)) {
                    continue;
                }
                field.setAccessible(true);
                Object fieldObject = field.get(object);

                verifyAllFieldsAreSet(fieldObject,
                        path + " -> " + field.getName() + ":" + field.getType().getCanonicalName());
            }
        }
    }

    private void failWithUnsetField(String path) {
        fail("Found null field or empty iterable (" + path + ") in test object. "
                + "This probably means that a new "
                + "field has been added to the class, but a value has not been set "
                + "for it in this test suite. This could result in changes to the "
                + "new field being ignored when calculating the hash or even the "
                + "entire hashing logic failing fast and defaulting to always "
                + "assuming the hash does not match. Ensure the new field is "
                + "covered by the tests here.");
    }

    private ImmutableList<Field> getFields(Object object) {
        ImmutableList.Builder<Field> fieldsBuilder = ImmutableList.builder();

        for (Class<?> clazz = object.getClass(); clazz != null; clazz = clazz.getSuperclass()) {
            if (clazz.equals(Object.class)) {
                break;
            }

            for (Field field : clazz.getDeclaredFields()) {
                fieldsBuilder.add(field);
            }
        }

        return fieldsBuilder.build();
    }

    private static class ExcludedFieldsTestClass {

        private final int used;

        @ExcludeFromObjectComparison
        private final int ignored;

        public ExcludedFieldsTestClass(int used, int ignored) {
            this.used = used;
            this.ignored = ignored;
        }
    }

    private static class DelegateIsExcludedFieldsClass {

        private final ExcludedFieldsTestClass excludedFieldsTestClass;
        private int field;

        public DelegateIsExcludedFieldsClass(ExcludedFieldsTestClass excludedFieldsTestClass, int field) {
            this.excludedFieldsTestClass = excludedFieldsTestClass;
            this.field = field;
        }
    }

    private static class SetTestClass {

        private final Set<Integer> set;

        public SetTestClass(Set<Integer> set) {
            this.set = set;
        }

    }

    private static class FieldIgnoringEqualsClass {

        private final int used;
        private final int ignored;

        public FieldIgnoringEqualsClass(int used, int ignored) {
            this.used = used;
            this.ignored = ignored;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            FieldIgnoringEqualsClass that = (FieldIgnoringEqualsClass) o;
            return used == that.used;
        }
    }

    private static class FieldsIgnoringEqualsClass extends FieldIgnoringEqualsClass {

        private final int used2;
        private final int ignored2;

        public FieldsIgnoringEqualsClass(int used, int ignored, int used2, int ignored2) {
            super(used, ignored);
            this.used2 = used2;
            this.ignored2 = ignored2;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            if (!super.equals(o)) return false;
            FieldsIgnoringEqualsClass that = (FieldsIgnoringEqualsClass) o;
            return used2 == that.used2;
        }
    }

}