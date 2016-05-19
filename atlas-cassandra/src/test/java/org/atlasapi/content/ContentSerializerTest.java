package org.atlasapi.content;

import java.io.File;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Locale;

import org.atlasapi.entity.Alias;
import org.atlasapi.entity.Id;
import org.atlasapi.entity.Identified;
import org.atlasapi.entity.Rating;
import org.atlasapi.entity.Review;
import org.atlasapi.entity.Serializer;
import org.atlasapi.equivalence.EquivalenceRef;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.segment.SegmentEvent;
import org.atlasapi.segment.SegmentRef;
import org.atlasapi.serialization.protobuf.ContentProtos;

import com.metabroadcast.common.intl.Countries;
import com.metabroadcast.common.time.DateTimeZones;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.joda.time.Interval;
import org.joda.time.LocalDate;
import org.junit.Test;

import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class ContentSerializerTest {

    private final Serializer<Content, ContentProtos.Content> serializer = new ContentSerializer(new ContentSerializationVisitor(
            new NoOpContentResolver()));

    @Test
    public void testDeSerializesBrand() {
        Brand brand = new Brand();
        setContainerProperties(brand);
        brand.setSeriesRefs(ImmutableSet.of(
                new SeriesRef(
                        Id.valueOf(123L),
                        brand.getSource(),
                        "sort",
                        1,
                        new DateTime(DateTimeZones.UTC),
                        null,
                        null
                )
        ));

        ContentProtos.Content serialized = serializer.serialize(brand);
        Content deserialized = serializer.deserialize(serialized);

        assertThat(deserialized, is(instanceOf(Brand.class)));
        Brand deserializedBrand = (Brand) deserialized;

        checkContainerProperties(deserializedBrand, brand);
        assertThat(deserializedBrand.getSeriesRefs(), is(brand.getSeriesRefs()));
    }

    @Test
    public void testDeSerializesSeries() {
        Series series = new Series();
        setContainerProperties(series);
        serializeAndCheck(series);

        series.setBrandRef(new BrandRef(Id.valueOf(1234L), series.getSource()));
        serializeAndCheck(series);

        series.setTotalEpisodes(5);
        serializeAndCheck(series);

        series.withSeriesNumber(3);
        serializeAndCheck(series);
    }

    @Test
    public void testDeSerializesItem() {
        Item item = new Item();
        setItemProperties(item);

        ContentProtos.Content serialized = serializer.serialize(item);
        Content deserialized = serializer.deserialize(serialized);

        assertThat(deserialized, is(instanceOf(Item.class)));
        Item deserializedItem = (Item) deserialized;
        checkItemProperties(deserializedItem, item);
    }

    @Test
    public void testDeSerializesItemWithActivelyPublishedFalse() {
        Item item = new Item();
        setItemProperties(item);
        item.setActivelyPublished(false);

        ContentProtos.Content serialized = serializer.serialize(item);
        Content deserialized = serializer.deserialize(serialized);

        assertThat(deserialized, is(instanceOf(Item.class)));
        Item deserializedItem = (Item) deserialized;
        checkItemProperties(deserializedItem, item);
    }

    @Test
    public void testDeSerializesEpisode() {
        Episode episode = new Episode();
        setItemProperties(episode);
        episode.setEpisodeNumber(5);
        episode.setPartNumber(4);
        episode.setSeriesNumber(5);
        SeriesRef seriesRef = new SeriesRef(
                Id.valueOf(5),
                episode.getSource(),
                "title",
                5,
                new DateTime(DateTimeZones.LONDON),
                null,
                null
        );
        episode.setSeriesRef(seriesRef);

        ContentProtos.Content serialized = serializer.serialize(episode);
        Content deserialized = serializer.deserialize(serialized);

        assertThat(deserialized, is(instanceOf(Episode.class)));
        Episode deserializedEpisode = (Episode) deserialized;
        checkItemProperties(deserializedEpisode, episode);
        assertThat(deserializedEpisode.getEpisodeNumber(), is(episode.getEpisodeNumber()));
        assertThat(deserializedEpisode.getPartNumber(), is(episode.getPartNumber()));
        assertThat(deserializedEpisode.getSeriesNumber(), is(episode.getSeriesNumber()));
        assertThat(deserializedEpisode.getSeriesRef(), is(episode.getSeriesRef()));
    }

    @Test
    public void testDeSerializes() {
        Film film = new Film();
        setItemProperties(film);
        serializeAndCheck(film);

        film.setReleaseDates(ImmutableSet.of(new ReleaseDate(
                new LocalDate(DateTimeZones.UTC),
                Countries.GB,
                ReleaseDate.ReleaseType.GENERAL
        )));
        serializeAndCheck(film);

        film.setWebsiteUrl("web url");
        serializeAndCheck(film);

        film.setSubtitles(ImmutableSet.of(new Subtitles("en-GB")));
        serializeAndCheck(film);
    }


    @Test
    public void testDeSerializesFilmWithReviewsAndRatings() {
        Film film = new Film();
        setItemProperties(film);
        addReviewsAndRatingsToDescribed(film);
        serializeAndCheck(film);
    }

    @Test
    public void testDeSerializesSong() {
        Song song = new Song();
        setItemProperties(song);
        serializeAndCheck(song);

        song.setIsrc("isrc");
        serializeAndCheck(song);
        song.setDuration(Duration.standardSeconds(1));
        serializeAndCheck(song);
    }

    @Test
    public void testDeserializationBackwardsCompatibility() throws Exception {
        File file = new File("src/test/resources/protoc/legacy-serialized-series.bin");
        byte[] msg = Files.readAllBytes(file.toPath());

        ContentProtos.Content deserialized = ContentProtos.Content.parseFrom(msg);
        Content actual = serializer.deserialize(deserialized);

        Series expected = new Series();
        setContainerProperties(expected);

        assertThat(actual, is(instanceOf(Series.class)));
        checkContainerProperties((Series) actual, expected);
    }

    private void serializeAndCheck(Series series) {
        ContentProtos.Content serialized = serializer.serialize(series);
        Content deserialized = serializer.deserialize(serialized);

        assertThat(deserialized, is(instanceOf(Series.class)));
        Series deserializedSeries = (Series) deserialized;

        checkContainerProperties(deserializedSeries, series);
        assertThat(deserializedSeries.getBrandRef(), is(series.getBrandRef()));
        assertThat(deserializedSeries.getTotalEpisodes(), is(series.getTotalEpisodes()));
        assertThat(deserializedSeries.getSeriesNumber(), is(series.getSeriesNumber()));
    }

    private void serializeAndCheck(Film film) {
        ContentProtos.Content serialized = serializer.serialize(film);
        Content deserialized = serializer.deserialize(serialized);

        assertThat(deserialized, is(instanceOf(Film.class)));
        Film deserializedFilm = (Film) deserialized;
        checkItemProperties(deserializedFilm, film);
        assertThat(deserializedFilm.getReleaseDates(), is(film.getReleaseDates()));
        assertThat(deserializedFilm.getWebsiteUrl(), is(film.getWebsiteUrl()));
        assertThat(deserializedFilm.getSubtitles(), is(film.getSubtitles()));
    }

    private void serializeAndCheck(Song song) {
        ContentProtos.Content serialized = serializer.serialize(song);
        Content deserialized = serializer.deserialize(serialized);

        assertThat(deserialized, is(instanceOf(Song.class)));
        Song deserializedSong = (Song) deserialized;
        checkItemProperties(deserializedSong, song);

        assertThat(deserializedSong.getIsrc(), is(song.getIsrc()));
        assertThat(deserializedSong.getDuration(), is(song.getDuration()));
    }

    private void checkContainerProperties(Container actual, Container expected) {
        checkContentProperties(actual, expected);
        assertThat(actual.getItemRefs(), is(expected.getItemRefs()));
        assertThat(actual.getUpcomingContent(), is(expected.getUpcomingContent()));
        assertThat(actual.getAvailableContent(), is(expected.getAvailableContent()));
        assertThat(actual.getItemSummaries(), is(expected.getItemSummaries()));
    }

    private void checkItemProperties(Item actual, Item expected) {
        checkContentProperties(actual, expected);
        assertThat(actual.getContainerRef(), is(expected.getContainerRef()));
        assertThat(
                actual.getContainerSummary().getTitle(),
                is(expected.getContainerSummary().getTitle())
        );
        assertThat(actual.getBlackAndWhite(), is(expected.getBlackAndWhite()));
        assertThat(actual.getCountriesOfOrigin(), is(expected.getCountriesOfOrigin()));
        assertThat(actual.getIsLongForm(), is(expected.getIsLongForm()));
        assertThat(actual.getBroadcasts().isEmpty(), is(false));
        assertThat(actual.getSegmentEvents().isEmpty(), is(false));
        assertThat(actual.getRestrictions().isEmpty(), is(false));
    }

    private void checkContentProperties(Content actual, Content expected) {
        checkDescribedProperties(actual, expected);
        assertThat(actual.people(), is(expected.people()));
        assertThat(actual.getCertificates(), is(expected.getCertificates()));
        assertThat(actual.getClips(), is(expected.getClips()));
        assertThat(actual.getContentGroupRefs(), is(expected.getContentGroupRefs()));
        assertThat(actual.getKeyPhrases(), is(expected.getKeyPhrases()));
        assertThat(actual.getLanguages(), is(expected.getLanguages()));
        assertThat(actual.getRelatedLinks(), is(expected.getRelatedLinks()));
        assertThat(actual.getTags(), is(expected.getTags()));
        assertThat(actual.getYear(), is(expected.getYear()));
        assertThat(actual.getManifestedAs().isEmpty(), is(false));
        assertThat(actual.isGenericDescription(), is(expected.isGenericDescription()));
    }

    private void checkDescribedProperties(Described actual, Described expected) {
        checkIdentifiedProperties(actual, expected);
        assertThat(actual.getSource(), is(expected.getSource()));
        assertThat(actual.getDescription(), is(expected.getDescription()));
        assertThat(actual.getFirstSeen(), is(expected.getFirstSeen()));
        assertThat(actual.getGenres(), is(expected.getGenres()));
        assertThat(actual.getImage(), is(expected.getImage()));
        assertThat(actual.getImages(), is(expected.getImages()));
        assertThat(actual.getLongDescription(), is(expected.getLongDescription()));
        assertThat(actual.getMediaType(), is(expected.getMediaType()));
        assertThat(actual.getMediumDescription(), is(expected.getMediumDescription()));
        assertThat(actual.getPresentationChannel(), is(expected.getPresentationChannel()));
        assertThat(actual.isScheduleOnly(), is(expected.isScheduleOnly()));
        assertThat(actual.getShortDescription(), is(expected.getShortDescription()));
        assertThat(actual.getSpecialization(), is(expected.getSpecialization()));
        assertThat(actual.getThisOrChildLastUpdated(), is(expected.getThisOrChildLastUpdated()));
        assertThat(actual.getThumbnail(), is(expected.getThumbnail()));
        assertThat(actual.getTitle(), is(expected.getTitle()));
        assertThat(actual.isActivelyPublished(), is(expected.isActivelyPublished()));

        assertThat("Same number of reviews", actual.getReviews().size(), is(expected.getReviews().size()));
        assertTrue("All reviews present", actual.getReviews().containsAll(expected.getReviews()));
        assertThat("Same number of ratings", actual.getRatings().size(), is(expected.getRatings().size()));
        assertTrue("All ratings present", actual.getRatings().containsAll(expected.getRatings()));
    }

    private void checkIdentifiedProperties(Identified actual, Identified expected) {
        assertThat(actual.getId(), is(expected.getId()));
        assertThat(actual.getAliases(), is(expected.getAliases()));
        assertThat(actual.getCanonicalUri(), is(expected.getCanonicalUri()));
        assertThat(actual.getEquivalentTo(), is(expected.getEquivalentTo()));
        assertThat(actual.getLastUpdated(), is(expected.getLastUpdated()));
    }

    private void setContainerProperties(Container container) {
        setContentProperties(container);
        container.setItemRefs(ImmutableSet.of(
                new ItemRef(
                        Id.valueOf(123L),
                        container.getSource(),
                        "sort",
                        DateTime.parse("2015-09-09T10:08:18.432Z")
                )
        ));

        ImmutableMap<ItemRef, Iterable<BroadcastRef>> upcomingContent = ImmutableMap.<ItemRef, Iterable<BroadcastRef>>builder()
                .put(
                        new ItemRef(
                                Id.valueOf(1),
                                container.getSource(),
                                "sort",
                                DateTime.parse("2015-09-09T10:08:18.432Z")
                        ),
                        ImmutableList.of(
                                new BroadcastRef(
                                        "slot:id1",
                                        Id.valueOf(2),
                                        new Interval(
                                                DateTime.parse("2015-09-09T11:08:18.563Z"),
                                                DateTime.parse("2015-09-09T12:08:18.564Z")
                                        )
                                ),
                                new BroadcastRef(
                                        "slot:id2",
                                        Id.valueOf(3),
                                        new Interval(
                                                DateTime.parse("2015-09-09T11:08:18.564Z"),
                                                DateTime.parse("2015-09-09T13:08:18.564Z")
                                        )
                                )
                        )
                ).put(
                        new ItemRef(
                                Id.valueOf(2),
                                container.getSource(),
                                "sort",
                                DateTime.parse("2015-09-09T10:08:18.432Z")
                        ),
                        ImmutableList.of(
                                new BroadcastRef(
                                        "slot:id4",
                                        Id.valueOf(2),
                                        new Interval(
                                                DateTime.parse("2015-09-09T11:08:18.564Z"),
                                                DateTime.parse("2015-09-09T12:08:18.564Z")
                                        )
                                ),
                                new BroadcastRef(
                                        "slot:id5",
                                        Id.valueOf(3),
                                        new Interval(
                                                DateTime.parse("2015-09-09T11:08:18.564Z"),
                                                DateTime.parse("2015-09-09T13:08:18.564Z")
                                        )
                                )
                        )
                ).build();
        container.setUpcomingContent(upcomingContent);

        ImmutableMap<ItemRef, Iterable<LocationSummary>> availableContent = ImmutableMap.<ItemRef, Iterable<LocationSummary>>builder()
                .put(
                        new ItemRef(
                                Id.valueOf(1),
                                container.getSource(),
                                "sort1",
                                DateTime.parse("2015-09-09T10:08:18.432Z")
                        ),
                        ImmutableList.of(
                                new LocationSummary(
                                        true,
                                        "item1location1",
                                        DateTime.parse("2015-09-09T09:08:18.593Z"),
                                        DateTime.parse("2015-09-09T11:08:18.593Z")
                                ),
                                new LocationSummary(
                                        true,
                                        "item1location2",
                                        DateTime.parse("2015-09-09T09:08:18.593Z"),
                                        DateTime.parse("2015-09-09T11:08:18.593Z")
                                )
                        )
                )
                .put(
                        new ItemRef(
                                Id.valueOf(2),
                                container.getSource(),
                                "sort1",
                                DateTime.parse("2015-09-09T10:08:18.432Z")
                        ),
                        ImmutableList.of(
                                new LocationSummary(
                                        true,
                                        "item2location1",
                                        DateTime.parse("2015-09-09T09:08:18.593Z"),
                                        DateTime.parse("2015-09-09T11:08:18.593Z")
                                )
                        )
                )
                .build();

        container.setAvailableContent(availableContent);
        container.setItemSummaries(
                ImmutableList.of(
                        new ItemSummary(
                                new ItemRef(
                                        Id.valueOf(2),
                                        container.getSource(),
                                        "sort1",
                                        DateTime.parse("2015-09-09T10:08:18.432Z")
                                ),
                                "Title",
                                "Description", null,
                                2012,
                                ImmutableList.of(new Certificate("PG", Countries.GB))
                        )
                )
        );
    }

    private void setItemProperties(Item item) {
        setContentProperties(item);
        item.setContainerRef(new BrandRef(Id.valueOf(4321), item.getSource()));
        item.setContainerSummary(new ContainerSummary("brand", "title", "description", null));
        item.setBlackAndWhite(true);
        item.setCountriesOfOrigin(ImmutableSet.of(Countries.GB));
        item.setIsLongForm(true);

        item.setBroadcasts(ImmutableSet.of(
                new Broadcast(
                        Id.valueOf(1),
                        DateTime.parse("2015-09-09T10:08:18.432Z"),
                        DateTime.parse("2015-09-09T10:08:18.432Z")
                ),
                new Broadcast(
                        Id.valueOf(2),
                        DateTime.parse("2015-09-09T10:08:18.432Z"),
                        DateTime.parse("2015-09-09T10:08:18.432Z")
                )
        ));
        item.setSegmentEvents(ImmutableSet.of(segmentEvent(10L)));
        item.setRestrictions(ImmutableSet.of(Restriction.from(14, "old")));
    }

    private void setContentProperties(Content content) {
        setDescribedProperties(content);
        content.setCertificates(ImmutableSet.of(new Certificate("PG", Countries.GB)));
        content.setClips(ImmutableSet.of(new Clip("clip", "clip", Publisher.BBC)));
        content.setContentGroupRefs(ImmutableSet.of(new ContentGroupRef(Id.valueOf(1234), "uri")));
        content.setKeyPhrases(ImmutableSet.of(new KeyPhrase("phrase", null)));
        content.setLanguages(ImmutableSet.of("en"));
        content.setPeople(ImmutableList.of(CrewMember.crewMember(
                "id",
                "Jim",
                "director",
                Publisher.BBC
        )));
        content.setRelatedLinks(ImmutableSet.of(RelatedLink.twitterLink("twitter").build()));
        content.setTags(ImmutableSet.of(new Tag(1L, 1.0f, true, Tag.Relationship.TRANSCRIPTION)));
        content.setManifestedAs(ImmutableSet.of(encoding("one")));
        content.setYear(1234);
        content.setGenericDescription(true);
    }

    private void setDescribedProperties(Described described) {
        setIdentifiedProperties(described);
        described.setPublisher(Publisher.BBC);
        described.setDescription("desc");
        described.setFirstSeen(DateTime.parse("2015-09-09T10:08:18.542Z"));
        described.setGenres(ImmutableSet.of("genre"));
        described.setImage("image");
        described.setImages(ImmutableSet.of(new Image("image")));
        described.setLongDescription("longDesc");
        described.setMediaType(MediaType.AUDIO);
        described.setMediumDescription("medDesc");
        described.setPresentationChannel("bbcone");
        described.setScheduleOnly(true);
        described.setShortDescription("shortDesc");
        described.setSpecialization(Specialization.RADIO);
        described.setThisOrChildLastUpdated(DateTime.parse("2015-09-09T10:08:18.543Z"));
        described.setThumbnail("thumbnail");
        described.setTitle("title");
    }

    private void addReviewsAndRatingsToDescribed(Described described) {
        described.setReviews(Arrays.asList(
                new Review(Locale.ENGLISH, "dog's bolls"),
                new Review(Locale.CHINESE, "hen hao"),
                new Review(Locale.FRENCH, "tres bien")
        ));

        described.setRatings(Arrays.asList(
                new Rating("5STAR", 3.0f, Publisher.RADIO_TIMES),
                new Rating("MOOSE", 1.0f, Publisher.BBC)
        ));
    }

    private void setIdentifiedProperties(Identified identified) {
        identified.setId(Id.valueOf(1234));
        identified.setLastUpdated(DateTime.parse("2015-09-09T10:08:18.432Z"));
        identified.setAliases(ImmutableSet.of(new Alias("a", "alias1"), new Alias("b", "alias2")));
        identified.setCanonicalUri("canonicalUri");
        identified.setEquivalenceUpdate(DateTime.parse("2015-09-09T10:08:18.432Z"));
        identified.setEquivalentTo(ImmutableSet.of(new EquivalenceRef(Id.valueOf(1),
                Publisher.BBC)));
    }

    private SegmentEvent segmentEvent(Long segmentId) {
        SegmentEvent event = new SegmentEvent();
        event.setSegment(new SegmentRef(Id.valueOf(segmentId), Publisher.BBC));
        return event;
    }

    private Encoding encoding(String uri) {
        Encoding encoding = new Encoding();
        encoding.setSource(uri);
        return encoding;
    }

}
