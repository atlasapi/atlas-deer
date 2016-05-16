package org.atlasapi.system.legacy;

import org.atlasapi.media.channel.Channel;
import org.atlasapi.media.channel.ChannelResolver;
import org.atlasapi.media.entity.Alias;
import org.atlasapi.media.entity.Actor;
import org.atlasapi.media.entity.CrewMember;
import org.atlasapi.media.entity.BlackoutRestriction;
import org.atlasapi.media.entity.Broadcast;
import org.atlasapi.media.entity.Identified;
import org.atlasapi.media.entity.Item;
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

import java.util.Arrays;
import java.util.Locale;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class LegacyContentTransformerTest {

    @Mock
    private ChannelResolver channelResolver;

    @Mock
    private LegacySegmentMigrator legacySegmentMigrator;

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
    public void testCopyBlackoutRestrictionOnBroadcasts() {
        String channelId = "channelId";
        Item legacy = new Item();
        legacy.setId(1L);
        legacy.setParentRef(new ParentRef("parentUrl", 2L));
        legacy.setPublisher(Publisher.PA);
        Version version = new Version();
        Broadcast broadcast = new Broadcast(channelId, DateTime.now(), DateTime.now().plusHours(1));
        broadcast.withId("sourceId");
        broadcast.setBlackoutRestriction(new BlackoutRestriction(true));
        version.setBroadcasts(ImmutableSet.of(broadcast));
        version.setRestriction(new Restriction());
        legacy.setVersions(ImmutableSet.of(version));
        legacy.setAliases(ImmutableSet.<Alias>of());
        Channel channel = mock(Channel.class);
        when(channelResolver.fromUri(channelId)).thenReturn(Maybe.just(channel));

        org.atlasapi.content.Item transformed = (org.atlasapi.content.Item) objectUnderTest.apply(
                legacy);

        assertThat(Iterables.getOnlyElement(transformed.getBroadcasts())
                .getBlackoutRestriction()
                .get()
                .getAll(), is(true));
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
        legacy.setAliases(ImmutableSet.<Alias>of());
        when(channelResolver.fromUri(channelId)).thenReturn(Maybe.<Channel>nothing());

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
        assertTrue(actual instanceof org.atlasapi.content.Item);

        assertEquals(expected.people().size(), actual.people().size());
        assertTrue(actual.people().containsAll(expected.people()));
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
        transformedItem = (org.atlasapi.content.Item) objectUnderTest.apply(legacyItem);
        assertThat(transformedItem.getReviews().size(), is(0));

        legacyItem = new Item();
        legacyItem.setId(2L);
        legacyItem.setReviews(Arrays.asList(
                new Review(Locale.CHINESE, "hen hao"),
                new Review(Locale.ENGLISH, "dog's bolls"),
                new Review(Locale.FRENCH, "tres bien")
        ));

        transformedItem = (org.atlasapi.content.Item) objectUnderTest.apply(legacyItem);
        assertThat(transformedItem.getReviews().size(), is(3));

        assertThat(transformedItem.getReviews().containsAll(Arrays.asList(
                new org.atlasapi.entity.Review(Locale.ENGLISH, "dog's bolls"),
                new org.atlasapi.entity.Review(Locale.CHINESE, "hen hao"),
                new org.atlasapi.entity.Review(Locale.FRENCH, "tres bien")
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
}