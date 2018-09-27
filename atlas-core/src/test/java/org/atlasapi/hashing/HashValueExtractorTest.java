package org.atlasapi.hashing;

import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Currency;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.stream.Collectors;

import com.google.common.collect.Iterables;
import com.google.common.collect.Multimap;
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
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.segment.SegmentEvent;
import org.atlasapi.segment.SegmentRef;

import com.metabroadcast.common.currency.Price;
import com.metabroadcast.common.intl.Countries;
import com.metabroadcast.common.media.MimeType;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.joda.time.Interval;
import org.joda.time.LocalDate;
import org.junit.Before;
import org.junit.Test;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

public class HashValueExtractorTest {
    
    private HashValueExtractor hashValueExtractor;

    @Before
    public void setUp() throws Exception {
        hashValueExtractor = HashValueExtractor.create();
    }

    @Test
    public void getValueSupportsAllPrimitives() throws Exception {
        Optional<String> value = hashValueExtractor.getValueToHash(new PrimitivesTestClass());

        assertThat(value.isPresent(), is(true));
        assertThat(value.get().isEmpty(), is(false));
    }

    @Test
    public void getValueSupportsAllPrimitiveWrappers() throws Exception {
        Optional<String> value = hashValueExtractor.getValueToHash(new PrimitiveWrappersTestClass());

        assertThat(value.isPresent(), is(true));
        assertThat(value.get().isEmpty(), is(false));
    }

    @Test
    public void getValueSupportsHashableFields() throws Exception {
        Optional<String> value = hashValueExtractor.getValueToHash(new HashableFieldTestClass());

        assertThat(value.isPresent(), is(true));
        assertThat(value.get().isEmpty(), is(false));
    }

    @Test
    public void getValueSupportsNullFields() throws Exception {
        Optional<String> value = hashValueExtractor.getValueToHash(new NullFieldTestClass());

        assertThat(value.isPresent(), is(true));
        assertThat(value.get().isEmpty(), is(false));
    }

    @Test
    public void getValueSupportsIterableFields() throws Exception {
        Optional<String> value = hashValueExtractor.getValueToHash(new IterableFieldTestClass());

        assertThat(value.isPresent(), is(true));
        assertThat(value.get().isEmpty(), is(false));
    }

    @Test
    public void getValueSupportsMapFields() throws Exception {
        Optional<String> value = hashValueExtractor.getValueToHash(new MapFieldTestClass());

        assertThat(value.isPresent(), is(true));
        assertThat(value.get().isEmpty(), is(false));
    }

    @Test
    public void getValueDoesNotSupportNonHashableFields() throws Exception {
        Optional<String> value = hashValueExtractor.getValueToHash(new NonHashableFieldTestClass());

        assertThat(value.isPresent(), is(false));
    }

    @Test
    public void getValueIgnoresStaticFields() throws Exception {
        Optional<String> value = hashValueExtractor.getValueToHash(new StaticFieldsTestClass());

        assertThat(value.isPresent(), is(true));
        assertThat(value.get().isEmpty(), is(false));
    }

    @Test
    public void getValueIgnoresExplicitlyExcludedFields() throws Exception {
        Optional<String> value = hashValueExtractor.getValueToHash(new ExcludedFieldsTestClass());

        assertThat(value.isPresent(), is(true));
        assertThat(value.get().isEmpty(), is(false));
    }

    @Test
    public void successiveCallsToGetValueProduceTheSameResult() throws Exception {
        PrimitivesTestClass testClass = new PrimitivesTestClass();

        String firstValue = hashValueExtractor.getValueToHash(testClass).get();
        String secondValue = hashValueExtractor.getValueToHash(testClass).get();

        assertThat(firstValue, is(secondValue));
    }

    @Test
    public void getValueProducesExpectedResult() throws Exception {
        AllSupportedFieldsTestClass testClass = new AllSupportedFieldsTestClass();

        String value = hashValueExtractor.getValueToHash(testClass).get();

        String expectedValue = ""
                + "org.atlasapi.hashing.HashValueExtractorTest.AllSupportedFieldsTestClass: { "
                + "org.atlasapi.hashing.HashValueExtractorTest.PrimitivesTestClass: { "
                + "java.lang.Boolean: { true } "
                + "| java.lang.Byte: { 0 } "
                + "| java.lang.Character: { c } "
                + "| java.lang.Short: { 1 } "
                + "| java.lang.Integer: { 2 } "
                + "| java.lang.Long: { 3 } "
                + "| java.lang.Float: { 4.0 } "
                + "| java.lang.Double: { 5.0 } "
                + "} "
                + "| org.atlasapi.hashing.HashValueExtractorTest.PrimitiveWrappersTestClass: { "
                + "java.lang.Boolean: { true } "
                + "| java.lang.Byte: { 0 } "
                + "| java.lang.Character: { c } "
                + "| java.lang.Short: { 1 } "
                + "| java.lang.Integer: { 2 } "
                + "| java.lang.Long: { 3 } "
                + "| java.lang.Float: { 4.0 } "
                + "| java.lang.Double: { 5.0 } "
                + "} "
                + "| org.atlasapi.hashing.HashValueExtractorTest.NullFieldTestClass: {  "
                + "} "
                + "| "
                + "org.atlasapi.hashing.HashValueExtractorTest.IterableFieldTestClass: { "
                + "com.google.common.collect.RegularImmutableList: {  "
                + "{ org.atlasapi.hashing.HashValueExtractorTest.PrimitivesTestClass: { "
                + "java.lang.Boolean: { true } "
                + "| java.lang.Byte: { 0 } "
                + "| java.lang.Character: { c } "
                + "| java.lang.Short: { 1 } "
                + "| java.lang.Integer: { 2 } "
                + "| java.lang.Long: { 3 } "
                + "| java.lang.Float: { 4.0 } "
                + "| java.lang.Double: { 5.0 } "
                + "}, "
                + "org.atlasapi.hashing.HashValueExtractorTest.PrimitivesTestClass: { "
                + "java.lang.Boolean: { true } "
                + "| java.lang.Byte: { 0 } "
                + "| java.lang.Character: { c } "
                + "| java.lang.Short: { 1 } "
                + "| java.lang.Integer: { 2 } "
                + "| java.lang.Long: { 3 } "
                + "| java.lang.Float: { 4.0 } "
                + "| java.lang.Double: { 5.0 } "
                + "} "
                + "} "
                + "} "
                + "| com.google.common.collect.RegularImmutableList: {  "
                + "{ "
                + "java.lang.String: { valueA }, "
                + "java.lang.String: { valueB } "
                + "} "
                + "} "
                + "} "
                + "| org.atlasapi.hashing.HashValueExtractorTest.MapFieldTestClass: { "
                + "com.google.common.collect.SingletonImmutableBiMap: {  "
                + "{ "
                + "java.lang.String: { key }"
                + "/"
                + "org.atlasapi.hashing.HashValueExtractorTest.PrimitivesTestClass: { "
                + "java.lang.Boolean: { true } "
                + "| java.lang.Byte: { 0 } "
                + "| java.lang.Character: { c } "
                + "| java.lang.Short: { 1 } "
                + "| java.lang.Integer: { 2 } "
                + "| java.lang.Long: { 3 } "
                + "| java.lang.Float: { 4.0 } "
                + "| java.lang.Double: { 5.0 } "
                + "} "
                + "} "
                + "} "
                + "} "
                + "}";

        assertThat(value, is(expectedValue));
    }

    @Test
    public void getValueSupportsAllFieldsInItem() throws Exception {
        Item content = new Item();

        setIdentifiedFields(content);
        setDescribedFields(content);
        setContentFields(content, true);
        setItemFields(content);

        verifyAllFieldsAreSet(content, "Item");

        Optional<String> hash = hashValueExtractor.getValueToHash(content);

        assertThat(hash.isPresent(), is(true));
        assertThat(hash.get().isEmpty(), is(false));
    }

    @Test
    public void getValueSupportsAllFieldsInSong() throws Exception {
        Song content = new Song();

        setIdentifiedFields(content);
        setDescribedFields(content);
        setContentFields(content, true);
        setItemFields(content);
        setSongFields(content);

        verifyAllFieldsAreSet(content, "Song");

        Optional<String> hash = hashValueExtractor.getValueToHash(content);

        assertThat(hash.isPresent(), is(true));
        assertThat(hash.get().isEmpty(), is(false));
    }

    @Test
    public void getValueSupportsAllFieldsInClip() throws Exception {
        Clip content = new Clip();

        setIdentifiedFields(content);
        setDescribedFields(content);
        setContentFields(content, true);
        setItemFields(content);
        setClipFields(content);

        verifyAllFieldsAreSet(content, "Clip");

        Optional<String> hash = hashValueExtractor.getValueToHash(content);

        assertThat(hash.isPresent(), is(true));
        assertThat(hash.get().isEmpty(), is(false));
    }

    @Test
    public void getValueSupportsAllFieldsInEpisode() throws Exception {
        Episode content = new Episode();

        setIdentifiedFields(content);
        setDescribedFields(content);
        setContentFields(content, true);
        setItemFields(content);
        setEpisodeFields(content);

        verifyAllFieldsAreSet(content, "Episode");

        Optional<String> hash = hashValueExtractor.getValueToHash(content);

        assertThat(hash.isPresent(), is(true));
        assertThat(hash.get().isEmpty(), is(false));
    }

    @Test
    public void getValueSupportsAllFieldsInFilm() throws Exception {
        Film content = new Film();

        setIdentifiedFields(content);
        setDescribedFields(content);
        setContentFields(content, true);
        setItemFields(content);
        setFilmFields(content);

        verifyAllFieldsAreSet(content, "Film");

        Optional<String> hash = hashValueExtractor.getValueToHash(content);

        assertThat(hash.isPresent(), is(true));
        assertThat(hash.get().isEmpty(), is(false));
    }

    @Test
    public void getValueSupportsAllFieldsInBrand() throws Exception {
        Brand content = new Brand();

        setIdentifiedFields(content);
        setDescribedFields(content);
        setContentFields(content, true);
        setContainerFields(content);
        setBrandFields(content);

        verifyAllFieldsAreSet(content, "Brand");

        Optional<String> hash = hashValueExtractor.getValueToHash(content);

        assertThat(hash.isPresent(), is(true));
        assertThat(hash.get().isEmpty(), is(false));
    }

    @Test
    public void getValueSupportsAllFieldsInSeries() throws Exception {
        Series content = new Series();

        setIdentifiedFields(content);
        setDescribedFields(content);
        setContentFields(content, true);
        setContainerFields(content);
        setSeriesFields(content);

        verifyAllFieldsAreSet(content, "Series");

        Optional<String> hash = hashValueExtractor.getValueToHash(content);

        assertThat(hash.isPresent(), is(true));
        assertThat(hash.get().isEmpty(), is(false));
    }

    private void setIdentifiedFields(Identified identified) {
        identified.setId(Id.valueOf(1L));
        identified.setCanonicalUri("canonicalUri");
        identified.setCurie("curie");
        identified.setAliasUrls(ImmutableSet.of("alias"));
        identified.setAliases(ImmutableSet.of(new Alias("ns", "value")));
        identified.setEquivalentTo(ImmutableSet.of(
                new EquivalenceRef(Id.valueOf(2L), Publisher.METABROADCAST))
        );
        identified.setLastUpdated(DateTime.now());
        identified.setEquivalenceUpdate(DateTime.now());
    }

    private void setDescribedFields(Described described) {
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
                .withAvailabilityStart(DateTime.now())
                .withAvailabilityEnd(DateTime.now())
                .withHasTitleArt(true)
                .withSource(Publisher.METABROADCAST)
                .build();
        described.setImages(ImmutableSet.of(image));

        described.setThumbnail("thumbnail");
        described.setFirstSeen(DateTime.now());
        described.setLastFetched(DateTime.now());
        described.setThisOrChildLastUpdated(DateTime.now());
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
        if (addClips) {
            Clip clip = new Clip();

            setIdentifiedFields(clip);
            setDescribedFields(clip);
            setContentFields(clip, false);
            setItemFields(clip);

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

        location.setAvailable(true);
        location.setTransportIsLive(false);
        location.setTransportSubType(TransportSubType.HTTP);
        location.setTransportType(TransportType.DOWNLOAD);
        location.setUri("uri");
        location.setEmbedCode("embed");

        Policy policy = new Policy();

        policy.setAvailabilityStart(DateTime.now());
        policy.setAvailabilityEnd(DateTime.now());
        policy.setDrmPlayableFrom(DateTime.now());
        policy.setAvailableCountries(ImmutableSet.of(Countries.GB));
        policy.setAvailabilityLength(10);
        policy.setRevenueContract(Policy.RevenueContract.FREE_TO_VIEW);
        policy.setSubscriptionPackages(ImmutableSet.of("subscription"));
        policy.setPrice(new Price(Currency.getInstance("GBP"), 10));
        policy.setPricing(ImmutableList.of(new Pricing(
                DateTime.now(), DateTime.now(), new Price(Currency.getInstance("GBP"), 10)
        )));
        policy.setServiceId(Id.valueOf(10L));
        policy.setPlayerId(Id.valueOf(11L));
        policy.setPlatform(Policy.Platform.BTVISION_CARDINAL);
        policy.setNetwork(Policy.Network.THREE_G);
        policy.setActualAvailabilityStart(DateTime.now());

        location.setPolicy(policy);

        encoding.setAvailableAt(ImmutableSet.of(location));

        content.setManifestedAs(ImmutableSet.of(encoding));
    }

    private void setItemFields(Item item) {
        item.setContainerRef(new BrandRef(Id.valueOf(10L), Publisher.METABROADCAST));
        item.setIsLongForm(true);
        item.setBlackAndWhite(false);
        item.setContainerSummary(ContainerSummary.create(
                "type", "title", "description", 5, 5
        ));

        Broadcast broadcast = new Broadcast(
                Id.valueOf(10L), DateTime.now(), DateTime.now(), true
        );
        setIdentifiedFields(broadcast);

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

        setIdentifiedFields(segmentEvent);

        item.setSegmentEvents(ImmutableSet.of(segmentEvent));

        Restriction restriction = new Restriction();

        restriction.setRestricted(true);
        restriction.setMinimumAge(5);
        restriction.setMessage("message");
        restriction.setAuthority("authority");
        restriction.setRating("rating");

        setIdentifiedFields(restriction);

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
        episode.setSeriesNumber(5);
        episode.setEpisodeNumber(5);
        episode.setPartNumber(1);
        episode.setSpecial(false);
        episode.setSeriesRef(new SeriesRef(
                Id.valueOf(0L),
                Publisher.METABROADCAST,
                "title",
                5,
                DateTime.now(),
                2000,
                ImmutableSet.of(new Certificate("18", Countries.GB))
        ));
    }

    private void setContainerFields(Container container) {
        ItemRef itemRef = new ItemRef(
                Id.valueOf(10L),
                Publisher.METABROADCAST,
                "sort",
                DateTime.now()
        );
        container.setItemRefs(ImmutableSet.of(itemRef));
        container.setUpcomingContent(ImmutableMap.of(
                itemRef,
                ImmutableSet.of(
                        new BroadcastRef(
                                "id",
                                Id.valueOf(1L),
                                new Interval(DateTime.now(), DateTime.now().plusDays(1))
                        )
                )
        ));
        container.setAvailableContent(ImmutableMap.of(
                itemRef,
                ImmutableSet.of(
                        new LocationSummary(
                                true,
                                "uri",
                                DateTime.now().minusDays(1),
                                DateTime.now().plusDays(1)
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
        brand.setSeriesRefs(ImmutableSet.of(new SeriesRef(
                Id.valueOf(1L),
                Publisher.METABROADCAST,
                "title",
                1,
                DateTime.now(),
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

    @SuppressWarnings("unused")
    private static class PrimitivesTestClass implements Hashable {

        private final boolean a = true;
        private final byte b = 0;
        private final char c = 'c';
        private final short d = 1;
        private final int e = 2;
        private final long f = 3L;
        private final float g = 4F;
        private final double h = 5D;
    }

    @SuppressWarnings("unused")
    private static class PrimitiveWrappersTestClass implements Hashable {

        private final Boolean aBoolean = true;
        private final Byte aByte = 0;
        private final Character aChar = 'c';
        private final Short aShort = 1;
        private final Integer anInteger = 2;
        private final Long aLong = 3L;
        private final Float aFloat = 4F;
        private final Double aDouble = 5D;
    }

    @SuppressWarnings("unused")
    private static class HashableFieldTestClass implements Hashable {

        private final Hashable field = new PrimitivesTestClass();
    }

    @SuppressWarnings("unused")
    private static class NullFieldTestClass implements Hashable {

        private final Integer fieldA = null;
        private final Hashable fieldB = null;
    }

    @SuppressWarnings("unused")
    private static class NonHashableFieldTestClass implements Hashable {

        private final NonHashableTestClass field = new NonHashableTestClass();
    }

    private static class NonHashableTestClass {

    }

    @SuppressWarnings("unused")
    private static class StaticFieldsTestClass implements Hashable {

        private static NonHashableTestClass field = new NonHashableTestClass();
    }

    @SuppressWarnings("unused")
    private static class ExcludedFieldsTestClass implements Hashable {

        @ExcludeFromHash
        private final NonHashableTestClass field = new NonHashableTestClass();
    }

    @SuppressWarnings("unused")
    private static class IterableFieldTestClass implements Hashable {

        private final ImmutableList<PrimitivesTestClass> fieldA = ImmutableList.of(
                new PrimitivesTestClass(),
                new PrimitivesTestClass()
        );

        private final ImmutableList<String> fieldB = ImmutableList.of(
                "valueA", "valueB"
        );
    }

    @SuppressWarnings("unused")
    private static class MapFieldTestClass implements Hashable {

        private final ImmutableMap<String, PrimitivesTestClass> field = ImmutableMap.of(
                "key",
                new PrimitivesTestClass()
        );
    }

    @SuppressWarnings("unused")
    private static class AllSupportedFieldsTestClass implements Hashable {

        private final PrimitivesTestClass primitives = new PrimitivesTestClass();
        private final PrimitiveWrappersTestClass wrappers = new PrimitiveWrappersTestClass();
        private final NullFieldTestClass nullFields = new NullFieldTestClass();
        private final IterableFieldTestClass iterableFields = new IterableFieldTestClass();
        private final MapFieldTestClass mapFields = new MapFieldTestClass();
    }
}
