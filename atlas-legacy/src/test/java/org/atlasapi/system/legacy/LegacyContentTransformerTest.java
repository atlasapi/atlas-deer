package org.atlasapi.system.legacy;

import java.util.Arrays;
import java.util.List;
import java.util.Locale;

import org.atlasapi.media.channel.Channel;
import org.atlasapi.media.channel.ChannelResolver;
import org.atlasapi.media.entity.Actor;
import org.atlasapi.media.entity.BlackoutRestriction;
import org.atlasapi.media.entity.Broadcast;
import org.atlasapi.media.entity.Clip;
import org.atlasapi.media.entity.CrewMember;
import org.atlasapi.media.entity.Described;
import org.atlasapi.media.entity.Encoding;
import org.atlasapi.media.entity.Identified;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.Location;
import org.atlasapi.media.entity.ParentRef;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.media.entity.Rating;
import org.atlasapi.media.entity.Restriction;
import org.atlasapi.media.entity.Review;
import org.atlasapi.media.entity.Series;
import org.atlasapi.media.entity.Version;

import com.metabroadcast.common.base.Maybe;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.joda.time.DateTime;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.Optional;

import static org.hamcrest.core.Is.is;
import static org.hamcrest.Matchers.greaterThan;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class LegacyContentTransformerTest {

    @Mock
    private ChannelResolver channelResolver;

    @Mock
    private LegacySegmentMigrator legacySegmentMigrator;

    @Mock
    private GenreToTagMapper genreToTagMapper;

    @InjectMocks
    private LegacyContentTransformer objectUnderTest;

    @Test
    public void testTransformSeriesWithParentRefWithNullId() {
        Series legacy = new Series();
        legacy.setId(1L);
        legacy.setParentRef(new ParentRef("parentUrl"));

        objectUnderTest.apply(legacy);
    }

    @Test
    public void broadcastFieldsAreTransformed() {
        Broadcast broadcast = new Broadcast(
                "channelId",
                DateTime.now(),
                DateTime.now().plusHours(1)
        ).withId("broadcastId");

        // Tested fields
        broadcast.setNewEpisode(true);
        broadcast.setBlackoutRestriction(new BlackoutRestriction(true));

        Version version = new Version();
        version.setBroadcasts(ImmutableSet.of(broadcast));
        version.setRestriction(new Restriction());

        Item legacyItem = new Item("uri", "", Publisher.METABROADCAST);
        legacyItem.setId(1L);
        legacyItem.setVersions(ImmutableSet.of(version));

        when(channelResolver.fromUri("channelId")).thenReturn(Maybe.just(mock(Channel.class)));

        org.atlasapi.content.Item transformed = (org.atlasapi.content.Item) objectUnderTest.apply(
                legacyItem);

        org.atlasapi.content.Broadcast actualBroadcast =
                Iterables.getOnlyElement(transformed.getBroadcasts());

        assertThat(
                actualBroadcast.getNewEpisode(),
                is(broadcast.getNewEpisode())
        );
        assertThat(
                actualBroadcast.getBlackoutRestriction().get().getAll(),
                is(broadcast.getBlackoutRestriction().getAll())
        );
    }

    @Test
    public void testIgnoreBroadcastsWithUnknownChannel() {
        String channelId = "nonexistentChannel";
        Item legacy = new Item();
        legacy.setId(1L);
        legacy.setParentRef(new ParentRef("parentUrl", 2L));
        legacy.setPublisher(Publisher.PA);
        Version version = new Version();
        Broadcast broadcast = new Broadcast(channelId, DateTime.now(), DateTime.now().plusHours(1));
        broadcast.withId("sourceId");
        version.setBroadcasts(ImmutableSet.of(broadcast));
        version.setRestriction(new Restriction());
        legacy.setVersions(ImmutableSet.of(version));
        legacy.setAliases(ImmutableSet.of());
        when(channelResolver.fromUri(channelId)).thenReturn(Maybe.nothing());

        org.atlasapi.content.Item transformed = (org.atlasapi.content.Item) objectUnderTest.apply(
                legacy);

        assertThat(transformed.getBroadcasts().size(), is(0));
    }

    @Test
    public void testContentWithPeople() {
        Item legacy = new Item();
        legacy.setId(666L);
        legacy.setPeople(Lists.newArrayList(
                fluentifySetId(new Actor(), 123L).withName("Robert Smith").withCharacter("Himself"),
                fluentifySetId(new CrewMember(), 321L).withName("Salesman McSaleFace").withRole(CrewMember.Role.ADVERTISER)
        ));

        org.atlasapi.content.Item expected = new org.atlasapi.content.Item();
        expected.setId(555L);
        expected.setPeople(Lists.newArrayList(
                fluentifySetId(new org.atlasapi.content.Actor(), 123L).withName("Robert Smith").withCharacter("Himself"),
                fluentifySetId(new org.atlasapi.content.CrewMember(), 321L).withName("Salesman McSaleFace").withRole(org.atlasapi.content.CrewMember.Role.ADVERTISER)
        ));

        org.atlasapi.content.Content actual = objectUnderTest.apply(legacy);
        assertThat(actual instanceof org.atlasapi.content.Item, is(true));

        assertThat(actual.people().size(), is(expected.people().size()));
        assertThat(actual.people().containsAll(expected.people()), is(true));
    }

    @Test
    public void testClip() {
        Clip legacy = new Clip("uri", "curie", Publisher.PREVIEW_NETWORKS);
        legacy.setTitle("trailer for film");
        legacy.setVersions(ImmutableSet.of(
                createVersionTestData("http://aka-m-p.maxplatform.com/15/59/28/xxlarge_640x360_HirsyF_1_uk_1_13826_27634_50544_1139.flv"),
                createVersionTestData("http://aka-m-p.maxplatform.com/15/59/28/HD-1080p_1920x1080_IchmLh_1_uk_1_13826_27634_50544_1139.webm")
        ));

        org.atlasapi.content.Clip expected = new org.atlasapi.content.Clip("uri", "curie", Publisher.PREVIEW_NETWORKS);
        expected.setTitle("trailer for film");

        org.atlasapi.content.Content actual = objectUnderTest.apply(legacy);
        assertThat(actual instanceof org.atlasapi.content.Clip, is(true));
        assertClipsEqual((org.atlasapi.content.Clip) actual, expected);
    }

    private Version createVersionTestData(String contentUrl) {
        Location location = new Location();
        location.setUri(contentUrl);
        Encoding encoding = new Encoding();
        encoding.addAvailableAt(location);
        Version result = new Version();
        result.setRestriction(new Restriction());
        result.addManifestedAs(encoding);
        return result;
    }

    @Test
    public void testContentWithClips() {
        Item legacy = new Item();
        legacy.setId(666L);
        legacy.setClips(Lists.newArrayList(
                fluentifySetTitle(new Clip("uri1", "curie", Publisher.PREVIEW_NETWORKS), "film trailer 1"),
                fluentifySetTitle(new Clip("uri2", "curie", Publisher.PREVIEW_NETWORKS), "film trailer 2")
        ));

        org.atlasapi.content.Item expected = new org.atlasapi.content.Item();
        expected.setClips(Lists.newArrayList(
                fluentifySetTitle(new org.atlasapi.content.Clip("uri2", "curie", Publisher.PREVIEW_NETWORKS), "film trailer 2"),
                fluentifySetTitle(new org.atlasapi.content.Clip("uri1", "curie", Publisher.PREVIEW_NETWORKS), "film trailer 1")
        ));

        org.atlasapi.content.Content actual = objectUnderTest.apply(legacy);
        assertThat(actual instanceof org.atlasapi.content.Item, is(true));

        assertAllClipsEqual(actual.getClips(), expected.getClips());

    }

    private void assertAllClipsEqual(List<org.atlasapi.content.Clip> actual, List<org.atlasapi.content.Clip> expected) {
        assertThat(actual.size(), is(expected.size()));

        List<org.atlasapi.content.Clip> expectedClips = Lists.newArrayList(expected);
        actual.stream().forEach(
                actualClip -> {

                    assertThat(expectedClips.size(), greaterThan(0));
                    for(int i = 0; i < expectedClips.size(); i++) {
                        org.atlasapi.content.Clip needle = expectedClips.get(i);
                        if (needle.equals(actualClip)) {
                            assertClipsEqual(actualClip, needle);
                            expectedClips.remove(i);
                            break;
                        }
                    }
                });

        assertThat("expectedClips has items that are not matched by actual.getClips()", expectedClips.size(), is(0));
    }

    private void assertClipsEqual(org.atlasapi.content.Clip actual, org.atlasapi.content.Clip expected) {
        // Clip.equals() is sparse, check fields that are referenced by API writer
        assertThat(actual.getTitle(), is(expected.getTitle()));
        assertThat(actual, is(expected));
    }


    private <C extends Identified> C fluentifySetId(C identifiedChild, long id) {
        identifiedChild.setId(id);
        return identifiedChild;
    }

    private <C extends org.atlasapi.entity.Identified> C fluentifySetId(C identifiedChild, long id) {
        identifiedChild.setId(id);
        return identifiedChild;
    }

    @Test
    public void testReviews() {
        Item legacyItem;
        org.atlasapi.content.Item transformedItem;

        legacyItem = new Item();
        legacyItem.setId(1L);
        legacyItem.setPublisher(Publisher.AMAZON_UK);
        transformedItem = (org.atlasapi.content.Item) objectUnderTest.apply(legacyItem);
        assertThat(transformedItem.getReviews().size(), is(0));

        legacyItem = new Item();
        legacyItem.setId(2L);
        legacyItem.setPublisher(Publisher.AMAZON_UK);
        legacyItem.setReviews(Arrays.asList(
                Review.builder().withLocale(Locale.CHINESE).withReview("hen hao").build(),
                Review.builder().withLocale(Locale.ENGLISH).withReview("dog's bolls").build(),
                Review.builder().withLocale(Locale.FRENCH).withReview("tres bien").build()
        ));

        transformedItem = (org.atlasapi.content.Item) objectUnderTest.apply(legacyItem);
        assertThat(transformedItem.getReviews().size(), is(3));

        assertThat(transformedItem.getReviews().containsAll(Arrays.asList(
                org.atlasapi.entity.Review.builder("hen hao").withLocale(Locale.CHINESE).withSource(Optional.of(Publisher.AMAZON_UK)).build(),
                org.atlasapi.entity.Review.builder("dog's bolls").withLocale(Locale.ENGLISH).withSource(Optional.of(Publisher.AMAZON_UK)).build(),
                org.atlasapi.entity.Review.builder("tres bien").withLocale(Locale.FRENCH).withSource(Optional.of(Publisher.AMAZON_UK)).build()
        )), is(true));
    }

    @Test
    public void testRatings() {
        Item legacyItem;
        org.atlasapi.content.Item transformedItem;

        legacyItem = new Item();
        legacyItem.setId(1L);
        transformedItem = (org.atlasapi.content.Item) objectUnderTest.apply(legacyItem);
        assertThat(transformedItem.getRatings().size(), is(0));

        legacyItem = new Item();
        legacyItem.setId(2L);
        legacyItem.setRatings(Arrays.asList(
                new Rating("5STAR", 3.0f, Publisher.RADIO_TIMES),
                new Rating("MOOSE", 1.0f, Publisher.BBC)
        ));

        transformedItem = (org.atlasapi.content.Item) objectUnderTest.apply(legacyItem);
        assertThat(transformedItem.getRatings().size(), is(2));

        assertThat(transformedItem.getRatings().containsAll(Arrays.asList(
                new org.atlasapi.entity.Rating("MOOSE", 1.0f, Publisher.BBC),
                new org.atlasapi.entity.Rating("5STAR", 3.0f, Publisher.RADIO_TIMES)
        )), is(true));
    }

    @Test(expected=NullPointerException.class)
    public void testWithBrokenReview() {
        org.atlasapi.content.Item transformedItem;

        Item legacyItem = new Item();
        legacyItem.setId(2L);
        legacyItem.setReviews(Arrays.asList(
                Review.builder().withLocale(Locale.ENGLISH).withReview(null).build()  // this is broken Review
        ));

        transformedItem = (org.atlasapi.content.Item) objectUnderTest.apply(legacyItem);
        assertThat(transformedItem.getReviews().size(), is(0));

        legacyItem.setReviews(Arrays.asList(
                Review.builder().withLocale(Locale.CHINESE).withReview("hen hao").build(),
                Review.builder().withLocale(Locale.ENGLISH).withReview( null).build()  // this is broken Review
        ));

        transformedItem = (org.atlasapi.content.Item) objectUnderTest.apply(legacyItem);
        assertThat(transformedItem.getReviews().size(), is(1));
    }

    private <C extends Described> C fluentifySetTitle(C describedChild, String title) {
        describedChild.setTitle(title);
        return describedChild;
    }

    private <C extends org.atlasapi.content.Described> C fluentifySetTitle(C describedChild, String title) {
        describedChild.setTitle(title);
        return describedChild;
    }
}
