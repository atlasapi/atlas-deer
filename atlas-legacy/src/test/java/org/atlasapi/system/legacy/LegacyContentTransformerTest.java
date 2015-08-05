package org.atlasapi.system.legacy;


import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.metabroadcast.common.base.Maybe;
import org.atlasapi.content.Content;
import org.atlasapi.media.channel.Channel;
import org.atlasapi.media.channel.ChannelResolver;
import org.atlasapi.media.entity.Alias;
import org.atlasapi.media.entity.BlackoutRestriction;
import org.atlasapi.media.entity.Broadcast;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.ParentRef;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.media.entity.Restriction;
import org.atlasapi.media.entity.Series;
import org.atlasapi.media.entity.Version;
import org.joda.time.DateTime;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
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

        org.atlasapi.content.Item transformed = (org.atlasapi.content.Item) objectUnderTest.apply(legacy);

        assertThat(Iterables.getOnlyElement(transformed.getBroadcasts()).getBlackoutRestriction().get().getAll(), is(true));
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

        org.atlasapi.content.Item transformed = (org.atlasapi.content.Item) objectUnderTest.apply(legacy);

        assertThat(transformed.getBroadcasts().size(), is(0));
    }
}