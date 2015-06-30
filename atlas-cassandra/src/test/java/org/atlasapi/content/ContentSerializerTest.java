package org.atlasapi.content;

import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import com.google.common.collect.ImmutableMap;
import org.atlasapi.entity.Alias;
import org.atlasapi.entity.Id;
import org.atlasapi.entity.Serializer;
import org.atlasapi.equivalence.EquivalenceRef;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.segment.SegmentEvent;
import org.atlasapi.segment.SegmentRef;
import org.atlasapi.serialization.protobuf.ContentProtos;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Duration;
import org.joda.time.Interval;
import org.joda.time.LocalDate;
import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.metabroadcast.common.intl.Countries;
import com.metabroadcast.common.time.DateTimeZones;


public class ContentSerializerTest {
    
    private final Serializer<Content,ContentProtos.Content> serializer = new ContentSerializer(new ContentSerializationVisitor(new NoOpContentResolver()));

    @Test
    public void testDeSerializesBrand() {
        Brand brand = new Brand();
        setContainerProperties(brand);
        brand.setSeriesRefs(ImmutableSet.of(
            new SeriesRef(Id.valueOf(123L), brand.getSource(), "sort", 1, new DateTime(DateTimeZones.UTC))
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

    @Test
    public void testDeSerializesItem() {
        Item item = new Item();
        setItemProperties(item);
        
        ContentProtos.Content serialized = serializer.serialize(item);
        Content deserialized = serializer.deserialize(serialized);
        
        assertThat(deserialized, is(instanceOf(Item.class)));
        Item deserializedItem = (Item)deserialized;
        checkItemProperties(deserializedItem, item);
    }
    
    
    @Test
    public void testDeSerializesEpisode() {
        Episode episode = new Episode();
        setItemProperties(episode);
        episode.setEpisodeNumber(5);
        episode.setPartNumber(4);
        episode.setSeriesNumber(5);
        SeriesRef seriesRef = new SeriesRef(Id.valueOf(5), episode.getSource(), "title", 5, new DateTime(DateTimeZones.LONDON));
        episode.setSeriesRef(seriesRef);
        
        ContentProtos.Content serialized = serializer.serialize(episode);
        Content deserialized = serializer.deserialize(serialized);
        
        assertThat(deserialized, is(instanceOf(Episode.class)));
        Episode deserializedEpisode = (Episode)deserialized;
        checkItemProperties(deserializedEpisode, episode);
        assertThat(deserializedEpisode.getEpisodeNumber(), is(episode.getEpisodeNumber()));
        assertThat(deserializedEpisode.getPartNumber(), is(episode.getPartNumber()));
        assertThat(deserializedEpisode.getSeriesNumber(), is(episode.getSeriesNumber()));
        assertThat(deserializedEpisode.getSeriesRef(), is(episode.getSeriesRef()));
    }
    
    @Test
    public void testDeSerializesFilm() {
        Film film = new Film();
        setItemProperties(film);
        serializeAndCheck(film);

        film.setReleaseDates(ImmutableSet.of(new ReleaseDate(new LocalDate(DateTimeZones.UTC), Countries.GB, ReleaseDate.ReleaseType.GENERAL)));
        serializeAndCheck(film);
        
        film.setWebsiteUrl("web url");
        serializeAndCheck(film);

        film.setSubtitles(ImmutableSet.of(new Subtitles("en-GB")));
        serializeAndCheck(film);
    }

    private void serializeAndCheck(Film film) {
        ContentProtos.Content serialized = serializer.serialize(film);
        Content deserialized = serializer.deserialize(serialized);
        
        assertThat(deserialized, is(instanceOf(Film.class)));
        Film deserializedFilm = (Film)deserialized;
        checkItemProperties(deserializedFilm, film);
        assertThat(deserializedFilm.getReleaseDates(), is(film.getReleaseDates()));
        assertThat(deserializedFilm.getWebsiteUrl(), is(film.getWebsiteUrl()));
        assertThat(deserializedFilm.getSubtitles(), is(film.getSubtitles()));
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

    private void serializeAndCheck(Song song) {
        ContentProtos.Content serialized = serializer.serialize(song);
        Content deserialized = serializer.deserialize(serialized);
        
        assertThat(deserialized, is(instanceOf(Song.class)));
        Song deserializedSong = (Song)deserialized;
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
        assertThat(actual.getContainerSummary().getTitle(), is(expected.getContainerSummary().getTitle()));
        assertThat(actual.getBlackAndWhite(), is(expected.getBlackAndWhite()));
        assertThat(actual.getCountriesOfOrigin(), is(expected.getCountriesOfOrigin()));
        assertThat(actual.getIsLongForm(), is(expected.getIsLongForm()));
        assertThat(actual.getBroadcasts().isEmpty(), is(false));
        assertThat(actual.getManifestedAs().isEmpty(), is(false));
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
        assertThat(actual.getTopicRefs(), is(expected.getTopicRefs()));
        assertThat(actual.getYear(), is(expected.getYear()));
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
            new ItemRef(Id.valueOf(123L), container.getSource(), "sort", new DateTime(DateTimeZones.UTC))
        ));

        ImmutableMap<ItemRef, Iterable<BroadcastRef>> upcomingContent = ImmutableMap.<ItemRef, Iterable<BroadcastRef>>builder()
                .put(
                        new ItemRef(
                                Id.valueOf(1),
                                container.getSource(),
                                "sort",
                                new DateTime(DateTimeZones.UTC)
                        ),
                        ImmutableList.of(
                                new BroadcastRef(
                                        "slot:id1",
                                        Id.valueOf(2),
                                        new Interval(
                                                DateTime.now(DateTimeZone.UTC).plusHours(1),
                                                DateTime.now(DateTimeZone.UTC).plusHours(2)
                                        )
                                ),
                                new BroadcastRef(
                                        "slot:id2",
                                        Id.valueOf(3),
                                        new Interval(
                                                DateTime.now(DateTimeZone.UTC).plusHours(1),
                                                DateTime.now(DateTimeZone.UTC).plusHours(3)
                                        )
                                )
                        )
                ).put(
                        new ItemRef(
                                Id.valueOf(2),
                                container.getSource(),
                                "sort",
                                new DateTime(DateTimeZones.UTC)
                        ),
                        ImmutableList.of(
                                new BroadcastRef(
                                        "slot:id4",
                                        Id.valueOf(2),
                                        new Interval(
                                                DateTime.now(DateTimeZone.UTC).plusHours(1),
                                                DateTime.now(DateTimeZone.UTC).plusHours(2)
                                        )
                                ),
                                new BroadcastRef(
                                        "slot:id5",
                                        Id.valueOf(3),
                                        new Interval(
                                                DateTime.now(DateTimeZone.UTC).plusHours(1),
                                                DateTime.now(DateTimeZone.UTC).plusHours(3)
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
                                new DateTime(DateTimeZones.UTC)
                        ),
                        ImmutableList.of(
                                new LocationSummary(
                                        true,
                                        "item1location1",
                                        DateTime.now(DateTimeZone.UTC).minusHours(1),
                                        DateTime.now(DateTimeZone.UTC).plusHours(1)
                                ),
                                new LocationSummary(
                                        true,
                                        "item1location2",
                                        DateTime.now(DateTimeZone.UTC).minusHours(1),
                                        DateTime.now(DateTimeZone.UTC).plusHours(1)
                                )
                        )
                )
                .put(
                        new ItemRef(
                                Id.valueOf(2),
                                container.getSource(),
                                "sort1",
                                new DateTime(DateTimeZones.UTC)
                        ),
                        ImmutableList.of(
                                new LocationSummary(
                                        true,
                                        "item2location1",
                                        DateTime.now(DateTimeZone.UTC).minusHours(1),
                                        DateTime.now(DateTimeZone.UTC).plusHours(1)
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
                                        new DateTime(DateTimeZones.UTC)
                                ),
                                "Title",
                                "Description", null
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
            new Broadcast(Id.valueOf(1), new DateTime(DateTimeZones.UTC), new DateTime(DateTimeZones.UTC)),
            new Broadcast(Id.valueOf(2), new DateTime(DateTimeZones.UTC), new DateTime(DateTimeZones.UTC))
        ));
        item.setManifestedAs(ImmutableSet.of(encoding("one")));
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
        content.setPeople(ImmutableList.of(CrewMember.crewMember("id", "Jim", "director", Publisher.BBC)));
        content.setRelatedLinks(ImmutableSet.of(RelatedLink.twitterLink("twitter").build()));
        content.setTopicRefs(ImmutableSet.of(new TopicRef(1L, 1.0f, true, TopicRef.Relationship.TRANSCRIPTION)));
        content.setYear(1234);
    }

    private void setDescribedProperties(Described described) {
        setIdentifiedProperties(described);
        described.setPublisher(Publisher.BBC);
        described.setDescription("desc");
        described.setFirstSeen(new DateTime(DateTimeZones.UTC));
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
        described.setThisOrChildLastUpdated(new DateTime(DateTimeZones.UTC));
        described.setThumbnail("thumbnail");
        described.setTitle("title");
    }

    private void setIdentifiedProperties(Identified identified) {
        identified.setId(Id.valueOf(1234));
        identified.setLastUpdated(new DateTime(DateTimeZones.UTC));
        identified.setAliases(ImmutableSet.of(new Alias("a","alias1"),new Alias("b","alias2")));
        identified.setCanonicalUri("canonicalUri");
        identified.setEquivalenceUpdate(new DateTime(DateTimeZones.UTC));
        identified.setEquivalentTo(ImmutableSet.of(new EquivalenceRef(Id.valueOf(1) ,Publisher.BBC)));
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
