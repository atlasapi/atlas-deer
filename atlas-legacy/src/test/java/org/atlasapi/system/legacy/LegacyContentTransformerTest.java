package org.atlasapi.system.legacy;


import com.google.common.collect.ImmutableSet;
import com.metabroadcast.common.base.Maybe;
import org.atlasapi.media.channel.Channel;
import org.atlasapi.media.channel.ChannelResolver;
import org.atlasapi.media.entity.Alias;
import org.atlasapi.media.entity.Broadcast;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.ParentRef;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.media.entity.Restriction;
import org.atlasapi.media.entity.Series;
import org.atlasapi.media.entity.Version;
import org.atlasapi.system.legacy.exception.LegacyChannelNotFoundException;
import org.joda.time.DateTime;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

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

    @Test(expected = LegacyChannelNotFoundException.class)
    public void testThrowLegacyChannelNotFoundExceptionWhenChannelNotFound() {
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

        objectUnderTest.apply(legacy);
    }

}