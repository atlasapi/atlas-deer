package org.atlasapi.system.legacy;

import java.util.concurrent.TimeUnit;

import org.atlasapi.channel.Channel;
import org.atlasapi.entity.Id;
import org.atlasapi.entity.util.Resolved;
import org.atlasapi.media.channel.ChannelQuery;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Futures;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.anyListOf;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class LegacyChannelResolverTest {

    @Mock
    private org.atlasapi.media.channel.ChannelResolver legacyResolver;
    @Mock
    private LegacyChannelTransformer transformer;

    @InjectMocks
    LegacyChannelResolver objectUnderTest;

    @Test
    public void testResolveChannels() throws Exception {
        String filterGenre = "filterGenre";

        org.atlasapi.media.channel.Channel legacyChannel1 = mock(org.atlasapi.media.channel.Channel.class);
        org.atlasapi.media.channel.Channel legacyChannel2 = mock(org.atlasapi.media.channel.Channel.class);
        org.atlasapi.media.channel.Channel legacyChannel3 = mock(org.atlasapi.media.channel.Channel.class);
        Iterable<org.atlasapi.media.channel.Channel> legacyChannels = ImmutableSet.of(
                legacyChannel1,
                legacyChannel2,
                legacyChannel3
        );

        Channel channel1 = mock(Channel.class);
        when(channel1.getGenres()).thenReturn(ImmutableSet.of(filterGenre, "someOtherGenre"));
        Channel channel2 = mock(Channel.class);
        when(channel2.getGenres()).thenReturn(ImmutableSet.of("someOtherGenre2"));
        Channel channel3 = mock(Channel.class);
        when(channel3.getGenres()).thenReturn(ImmutableSet.of(filterGenre));

        when(transformer.apply(legacyChannel1)).thenReturn(channel1);
        when(transformer.apply(legacyChannel2)).thenReturn(channel2);
        when(transformer.apply(legacyChannel3)).thenReturn(channel3);

        ChannelQuery channelQuery = mock(ChannelQuery.class);
        when(legacyResolver.allChannels(channelQuery)).thenReturn(legacyChannels);

        Iterable<Channel> result = Futures.get(
                Futures.transform(
                        objectUnderTest.resolveChannels(channelQuery),
                        new Function<Resolved<Channel>, Iterable<Channel>>() {

                            @Override
                            public Iterable<Channel> apply(Resolved<Channel> input) {
                                return input.getResources();
                            }
                        }
                ), 1, TimeUnit.MINUTES, Exception.class
        );

        assertThat(Lists.newArrayList(result), containsInAnyOrder(channel1, channel2, channel3));
    }

    @Test
    public void testResolveIds() throws Exception {
        Id id1 = Id.valueOf(1);
        Id id2 = Id.valueOf(2);
        Id id3 = Id.valueOf(3);

        org.atlasapi.media.channel.Channel legacyChannel1 = mock(org.atlasapi.media.channel.Channel.class);
        org.atlasapi.media.channel.Channel legacyChannel2 = mock(org.atlasapi.media.channel.Channel.class);
        org.atlasapi.media.channel.Channel legacyChannel3 = mock(org.atlasapi.media.channel.Channel.class);
        Iterable<org.atlasapi.media.channel.Channel> legacyChannels = ImmutableSet.of(
                legacyChannel1,
                legacyChannel2,
                legacyChannel3
        );

        Channel channel1 = mock(Channel.class);
        Channel channel2 = mock(Channel.class);
        Channel channel3 = mock(Channel.class);

        Iterable<Id> ids = ImmutableList.of(id1, id2, id3);

        when(legacyResolver.forIds(anyListOf(Long.class))).thenReturn(legacyChannels);

        when(transformer.apply(legacyChannel1)).thenReturn(channel1);
        when(transformer.apply(legacyChannel2)).thenReturn(channel2);
        when(transformer.apply(legacyChannel3)).thenReturn(channel3);

        Iterable<Channel> result = Futures.get(
                Futures.transform(
                        objectUnderTest.resolveIds(ids),
                        new Function<Resolved<Channel>, Iterable<Channel>>() {

                            @Override
                            public Iterable<Channel> apply(Resolved<Channel> input) {
                                return input.getResources();
                            }
                        }
                ), 1, TimeUnit.MINUTES, Exception.class
        );

        assertThat(Lists.newArrayList(result), containsInAnyOrder(channel1, channel2, channel3));

    }
}