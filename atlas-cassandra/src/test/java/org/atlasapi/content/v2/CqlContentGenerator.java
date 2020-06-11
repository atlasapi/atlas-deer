package org.atlasapi.content.v2;

import java.util.Currency;
import java.util.List;
import java.util.Locale;
import java.util.Optional;

import org.atlasapi.channel.Channel;
import org.atlasapi.content.Actor;
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
import org.atlasapi.content.Description;
import org.atlasapi.content.Encoding;
import org.atlasapi.content.Episode;
import org.atlasapi.content.Film;
import org.atlasapi.content.Image;
import org.atlasapi.content.Item;
import org.atlasapi.content.ItemRef;
import org.atlasapi.content.ItemSummary;
import org.atlasapi.content.KeyPhrase;
import org.atlasapi.content.LocalizedTitle;
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
import org.apache.activemq.broker.region.Topic;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Duration;
import org.joda.time.Interval;
import org.joda.time.LocalDate;

public class CqlContentGenerator {

    public static List<Content> makeContent() {
        return ImmutableList.of(
                makeItem(),
                makeSong(),
                makeEpisode(),
                makeFilm(),
                makeBrand(),
                makeSeries()
        );
    }

    private static void setIdentifiedFields(Identified c) {
        c.setId(Id.valueOf(42));
        c.setCanonicalUri("canonical uri");
        c.setCurie("curie");
        c.setAliasUrls(ImmutableList.of("some alias url"));
        c.setAliases(ImmutableList.of(new Alias("namespace", "value")));
        c.setEquivalentTo(ImmutableSet.of(new EquivalenceRef(Id.valueOf(43), Publisher.BBC_KIWI)));
        c.setLastUpdated(DateTime.now(DateTimeZone.UTC).minusHours(2));
        c.setEquivalenceUpdate(DateTime.now(DateTimeZone.UTC).minusHours(3));
    }

    private static void setContentFields(Content c) {
        setIdentifiedFields(c);

        c.setTitle("title");
        LocalizedTitle localizedTitle = new LocalizedTitle();
        localizedTitle.setTitle("titlu");
        localizedTitle.setLocale("ro","RO");
        LocalizedTitle localizedTitle1 = new LocalizedTitle();
        localizedTitle1.setTitle("titolo");
        c.setLocalizedTitles(ImmutableSet.of(localizedTitle,localizedTitle1));

        c.setShortDescription("short description");
        c.setMediumDescription("medium description");
        c.setLongDescription("long description");

        Synopses synopses = new Synopses();
        synopses.setShortDescription("short synopses description");
        synopses.setMediumDescription("medium synopses description");
        synopses.setLongDescription("long synopses description");
        c.setSynopses(synopses);

        c.setDescription("description");
        c.setMediaType(MediaType.VIDEO);
        c.setSpecialization(Specialization.TV);

        c.setGenres(ImmutableList.of("genre one", "genre two"));
        c.setPublisher(Publisher.BBC_KIWI);
        c.setImage("some image");
        c.setImages(ImmutableList.of(makeImage()));
        c.setThumbnail("a thumbnail (that probably hurts)");

        c.setFirstSeen(DateTime.now(DateTimeZone.UTC).minusDays(42));
        c.setLastFetched(DateTime.now(DateTimeZone.UTC).minusDays(1));
        c.setThisOrChildLastUpdated(DateTime.now(DateTimeZone.UTC).minusDays(2));
        c.setScheduleOnly(false);
        c.setActivelyPublished(true);

        c.setPresentationChannel(Channel.ADULT_GENRE);

        c.setPriority(new Priority(0.42, new PriorityScoreReasons(
                ImmutableList.of("kind of nice"),
                ImmutableList.of("bit dull")
        )));

        c.setRelatedLinks(ImmutableList.of(
                RelatedLink.facebookLink("doop a loop")
                        .withShortName("short short")
                        .withDescription("some description")
                        .withImage("image")
                        .withSourceId("source id")
                        .withThumbnail("thumb")
                        .withTitle("title title")
                        .build()
        ));

        Award award = new Award();
        award.setOutcome("almost won");
        award.setTitle("giant golden dude");
        award.setDescription("super important");
        award.setYear(2015);

        c.setAwards(ImmutableSet.of(award));

        if (!Clip.class.isInstance(c)) {
            c.setClips(ImmutableList.of(makeClip()));
        }

        c.setKeyPhrases(ImmutableList.of(new KeyPhrase("phrase phrase", 0.43)));
        c.setTags(ImmutableList.of(new Tag(
                Topic.DEFAULT_INACTIVE_TIMEOUT_BEFORE_GC,
                0.42f,
                Boolean.FALSE,
                Tag.Relationship.ABOUT
        )));

        c.setContentGroupRefs(ImmutableList.of(new ContentGroupRef(Id.valueOf(4), "uri uri")));

        c.setPeople(makeCrewMembers());

        c.setLanguages(ImmutableList.of("en", "de"));

        c.setCertificates(ImmutableList.of(new Certificate("idunnolol", Countries.GB)));

        c.setYear(2016);

        c.setManifestedAs(ImmutableSet.of(makeEncoding()));
        c.setGenericDescription(Boolean.FALSE);
        c.setEventRefs(ImmutableSet.of(new EventRef(Id.valueOf(23), Publisher.ARQIVA)));

        c.setRatings(ImmutableList.of(new Rating("sometype", 4.2f, Publisher.AMAZON_UK, 1234L)));

        c.setReviews(ImmutableList.of(
                Review.builder("hao aboot this one, eh?")
                .withLocale(Locale.CANADA)
                .withSource(Optional.of(Publisher.BBC_KIWI))
                .build()
        ));

        c.setCountriesOfOrigin(ImmutableSet.of(Countries.GB));
    }

    private static void setContainerFields(Container container) {
        setContentFields(container);

        container.setItemRefs(ImmutableList.of(new ItemRef(
                Id.valueOf(8775),
                Publisher.BT,
                "sort key",
                DateTime.now(DateTimeZone.UTC)
        )));

        container.setUpcomingContent(ImmutableMap.of(
                new ItemRef(
                        Id.valueOf(221),
                        Publisher.BT,
                        "sort key",
                        DateTime.now(DateTimeZone.UTC)
                ),
                ImmutableList.of(new BroadcastRef("source id", Id.valueOf(32199),
                        new Interval(
                                DateTime.now(DateTimeZone.UTC).minusHours(2),
                                DateTime.now(DateTimeZone.UTC)
                        )
                ))
        ));

        container.setAvailableContent(ImmutableMap.of(
                new ItemRef(
                        Id.valueOf(23087),
                        Publisher.BT,
                        "sort key",
                        DateTime.now(DateTimeZone.UTC)
                ),
                ImmutableList.of(new LocationSummary(
                        Boolean.TRUE,
                        "uri",
                        DateTime.now(DateTimeZone.UTC).minusHours(1),
                        DateTime.now(DateTimeZone.UTC)
                ))
        ));

        container.setItemSummaries(ImmutableList.of(new ItemSummary(
                new ItemRef(
                        Id.valueOf(32),
                        Publisher.BT,
                        "sort key",
                        DateTime.now(DateTimeZone.UTC)
                ),
                "title",
                "description",
                "image",
                2015,
                ImmutableList.of(new Certificate("class", Countries.GB))
        )));
    }

    private static Encoding makeEncoding() {
        Encoding encoding = new Encoding();

        setIdentifiedFields(encoding);

        encoding.setAvailableAt(ImmutableSet.of(makeLocation()));
        encoding.setContainsAdvertising(Boolean.FALSE);
        encoding.setAdvertisingDuration(322);
        encoding.setDuration(Duration.standardHours(2));

        encoding.setBitRate(256);

        encoding.setAudioBitRate(2321);
        encoding.setAudioChannels(2);
        encoding.setAudioCoding(MimeType.AUDIO_AC3);

        encoding.setVideoAspectRatio("16:10");
        encoding.setVideoBitRate(2311);
        encoding.setVideoCoding(MimeType.VIDEO_H264);
        encoding.setVideoFrameRate(0.222f);
        encoding.setVideoHorizontalSize(12222);
        encoding.setVideoProgressiveScan(Boolean.FALSE);
        encoding.setVideoVerticalSize(112341);

        encoding.setDataSize(43L);
        encoding.setDataContainerFormat(MimeType.APPLICATION_DASH);

        encoding.setSource("source");
        encoding.setDistributor("distributor");

        encoding.setHasDOG(Boolean.FALSE); // only has a CAT
        encoding.set3d(Boolean.FALSE);

        encoding.setQuality(Quality.FOUR_K);
        encoding.setQualityDetail("quality detail");
        encoding.setVersionId("version id");

        return encoding;
    }

    private static Location makeLocation() {
        Location location = new Location();

        setIdentifiedFields(location);

        location.setAvailable(true);
        location.setTransportIsLive(Boolean.TRUE);
        location.setTransportSubType(TransportSubType.HTTP);
        location.setTransportType(TransportType.DOWNLOAD);
        location.setUri("uri uri");
        location.setEmbedCode("embed code");
        location.setEmbedId("embed id");
        location.setPolicy(makePolicy());

        return location;
    }

    private static Policy makePolicy() {
        Policy policy = new Policy();

        setIdentifiedFields(policy);

        policy.setAvailabilityStart(DateTime.now(DateTimeZone.UTC).minusHours(20));
        policy.setAvailabilityEnd(DateTime.now(DateTimeZone.UTC).minusHours(15));
        policy.setDrmPlayableFrom(DateTime.now(DateTimeZone.UTC));

        policy.setAvailableCountries(ImmutableSet.of(Countries.FR));
        policy.setAvailabilityLength(2111);
        policy.setRevenueContract(Policy.RevenueContract.FREE_TO_VIEW);
        policy.setSubscriptionPackages(ImmutableList.of("package one", "package two"));

        policy.setPrice(new Price(Currency.getInstance("GBP"), 42));
        policy.setPricing(ImmutableList.of(new Pricing(
                DateTime.now(DateTimeZone.UTC).minusHours(2),
                DateTime.now(DateTimeZone.UTC),
                new Price(Currency.getInstance("GBP"), 43)
        )));

        policy.setServiceId(Id.valueOf(44));
        policy.setPlayerId(Id.valueOf(113));

        policy.setPlatform(Policy.Platform.PC);
        policy.setNetwork(Policy.Network.THREE_G);
        policy.setActualAvailabilityStart(DateTime.now(DateTimeZone.UTC));

        return policy;
    }

    private static ImmutableList<CrewMember> makeCrewMembers() {
        CrewMember crewMember0 = new CrewMember();
        setIdentifiedFields(crewMember0);

        CrewMember crewMember1 = new CrewMember();
        setIdentifiedFields(crewMember1);

        Actor actor = new Actor();
        setIdentifiedFields(actor);

        return ImmutableList.of(
                actor.withCharacter("Didneyworl"),
                crewMember0.withRole(CrewMember.Role.ADAPTOR)
                        .withName("Crew Member McMemberson")
                        .withPublisher(null),
                crewMember1.withRole(CrewMember.Role.ACTOR)
                        .withName("Crew Member")
                        .withPublisher(Publisher.BETTY)
        );
    }

    private static Clip makeClip() {
        Clip clip = new Clip();

        setContentFields(clip);
        setItemFields(clip);

        clip.setClipOf("clip of");

        return clip;
    }

    private static Image makeImage() {
        Image image = new Image("image uri");

        image.setType(Image.Type.PRIMARY);
        image.setColor(Image.Color.COLOR);
        image.setTheme(Image.Theme.LIGHT_OPAQUE);
        image.setHeight(138);
        image.setWidth(1422);
        image.setAspectRatio(Image.AspectRatio.FOUR_BY_THREE);
        image.setMimeType(MimeType.APPLICATION_ATOM_XML);
        image.setAvailabilityStart(DateTime.now(DateTimeZone.UTC).minusHours(3));
        image.setAvailabilityEnd(DateTime.now(DateTimeZone.UTC).minusHours(1));
        image.setHasTitleArt(Boolean.TRUE);
        image.setSource(Publisher.AMAZON_UK);

        return image;
    }

    private static void setItemFields(Item i) {
        setContentFields(i);

        i.setContainerRef(new BrandRef(Id.valueOf(222), Publisher.BBC));
        i.setIsLongForm(true);
        i.setBlackAndWhite(Boolean.FALSE);
        i.setCountriesOfOrigin(ImmutableSet.of(Countries.IE));
        i = i.withSortKey("sort key");
        i.setContainerSummary(ContainerSummary.create("type", "title", "description", 1, 10));
        i.setBroadcasts(ImmutableSet.of(makeBroadcast()));
        i.setSegmentEvents(ImmutableList.of(makeSegmentEvent()));
        i.setRestrictions(ImmutableSet.of(makeRestriction()));
        i.setDuration(Duration.standardHours(1));
    }

    private static Restriction makeRestriction() {
        Restriction restriction = new Restriction();

        setIdentifiedFields(restriction);

        restriction.setRestricted(Boolean.TRUE);
        restriction.setMinimumAge(2341);
        restriction.setMessage("message");
        restriction.setAuthority("authority");
        restriction.setRating("rating");

        return restriction;
    }

    private static SegmentEvent makeSegmentEvent() {
        SegmentEvent segmentEvent = new SegmentEvent();

        setIdentifiedFields(segmentEvent);

        segmentEvent.setPosition(23);
        segmentEvent.setOffset(Duration.standardHours(2));
        segmentEvent.setIsChapter(Boolean.TRUE);
        segmentEvent.setDescription(new Description("title", "synopsis", "image", "thumb"));
        segmentEvent.setSegment(new SegmentRef(Id.valueOf(2322), Publisher.BT_TV_CHANNELS_TEST2));
        segmentEvent.setVersionId("version id");
        segmentEvent.setPublisher(Publisher.BT_EVENTS);

        return segmentEvent;
    }

    private static Broadcast makeBroadcast() {
        Broadcast broadcast = new Broadcast(
                Id.valueOf(4421),
                DateTime.now(DateTimeZone.UTC).minusHours(2),
                DateTime.now(DateTimeZone.UTC).minusHours(1),
                Boolean.TRUE
        );

        setIdentifiedFields(broadcast);

        broadcast.setScheduleDate(LocalDate.now());
        broadcast = broadcast.withId("source id");
        broadcast.setVersionId("version ID");
        broadcast.setRepeat(Boolean.FALSE);
        broadcast.setSubtitled(Boolean.TRUE);
        broadcast.setSigned(Boolean.TRUE);
        broadcast.setAudioDescribed(Boolean.TRUE);
        broadcast.setHighDefinition(Boolean.TRUE);
        broadcast.setWidescreen(Boolean.TRUE);
        broadcast.setSurround(Boolean.FALSE);
        broadcast.setLive(Boolean.TRUE);
        broadcast.setNewSeries(Boolean.TRUE);
        broadcast.setNewEpisode(Boolean.TRUE);
        broadcast.setPremiere(Boolean.TRUE);
        broadcast.set3d(Boolean.TRUE);
        broadcast.setBlackoutRestriction(new BlackoutRestriction(Boolean.TRUE));

        return broadcast;
    }

    private static Series makeSeries() {
        Series series = new Series();

        setContainerFields(series);

        series = series.withSeriesNumber(3);
        series.setTotalEpisodes(33);
        series.setBrandRef(new BrandRef(Id.valueOf(210), Publisher.C4));

        return series;
    }

    private static Brand makeBrand() {
        Brand brand = new Brand();

        setContainerFields(brand);

        brand.setSeriesRefs(ImmutableList.of(new SeriesRef(
                Id.valueOf(2322),
                Publisher.C4,
                "title",
                32,
                DateTime.now(DateTimeZone.UTC),
                2015,
                ImmutableList.of(new Certificate("some kind of class", Countries.FR))
        )));

        return brand;
    }

    private static Film makeFilm() {
        Film film = new Film();

        setItemFields(film);

        film.setWebsiteUrl("website url");
        film.setSubtitles(ImmutableSet.of(new Subtitles("en"), new Subtitles("es")));
        film.setReleaseDates(ImmutableList.of(
                new ReleaseDate(LocalDate.now(), Countries.GB, ReleaseDate.ReleaseType.GENERAL)
        ));

        return film;
    }

    private static Episode makeEpisode() {
        Episode episode = new Episode();

        setItemFields(episode);

        episode.setSeriesNumber(3);
        episode.setEpisodeNumber(21);
        episode.setPartNumber(119);
        episode.setSpecial(Boolean.TRUE);
        episode.setSeriesRef(new SeriesRef(Id.valueOf(3191),
                Publisher.C4,
                "title",
                22,
                DateTime.now(DateTimeZone.UTC),
                2016,
                ImmutableList.of(new Certificate("class", Countries.FR))
        ));

        return episode;
    }

    private static Song makeSong() {
        Song song = new Song();

        setItemFields(song);

        song.setIsrc("some isrc");

        return song;
    }

    private static Item makeItem() {
        Item item = new Item();

        setItemFields(item);

        return item;
    }
}