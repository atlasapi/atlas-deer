package org.atlasapi.output.writers;

import org.atlasapi.channel.ChannelGroupResolver;
import org.atlasapi.channel.ChannelResolver;
import org.atlasapi.content.Broadcast;
import org.atlasapi.entity.Id;
import org.atlasapi.entity.util.Resolved;
import org.atlasapi.output.FieldWriter;
import org.atlasapi.output.OutputContext;

import com.metabroadcast.common.ids.NumberToShortStringCodec;

import com.google.common.util.concurrent.Futures;
import org.joda.time.DateTime;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import static org.mockito.Matchers.anyCollectionOf;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class BroadcastWriterTest {

    @Mock private NumberToShortStringCodec codec;
    @Mock private ChannelResolver channelResolver;

    @Mock private FieldWriter writer;
    @Mock private OutputContext context;

    private BroadcastWriter broadcastWriter;

    private Broadcast broadcast;

    @Before
    public void setUp() throws Exception {
        broadcastWriter = BroadcastWriter.create(
                "list", "field", codec, channelResolver
        );

        when(channelResolver.resolveIds(anyCollectionOf(Id.class)))
                .thenReturn(Futures.immediateFuture(Resolved.empty()));

        broadcast = new Broadcast(
                Id.valueOf(0L),
                DateTime.now(),
                DateTime.now().plusMinutes(10)
        );
        broadcast.setNewEpisode(true);
    }

    @Test
    public void writeNewEpisode() throws Exception {
        broadcastWriter.write(broadcast,  writer, context);

        verify(writer).writeField("new_episode", true);
    }
}
