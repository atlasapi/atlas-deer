package org.atlasapi.output.writers;

import javax.servlet.http.HttpServletRequest;

import org.atlasapi.annotation.Annotation;
import org.atlasapi.channel.Channel;
import org.atlasapi.channel.ResolvedChannel;
import org.atlasapi.content.Broadcast;
import org.atlasapi.content.ResolvedBroadcast;
import org.atlasapi.entity.Id;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.output.FieldWriter;
import org.atlasapi.output.OutputContext;

import com.metabroadcast.common.ids.NumberToShortStringCodec;

import org.joda.time.DateTime;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class BroadcastWriterTest {

    @Mock private NumberToShortStringCodec codec;

    @Mock private FieldWriter writer;
    @Mock private OutputContext context;
    @Mock HttpServletRequest request;

    private BroadcastWriter broadcastWriter;
    private String annotations = "annotations";

    private ResolvedBroadcast resolvedBroadcast;

    @Before
    public void setUp() throws Exception {
        broadcastWriter = BroadcastWriter.create(
                "list", "field", codec
        );

        Broadcast broadcast = new Broadcast(
                Id.valueOf(0L),
                DateTime.now(),
                DateTime.now().plusMinutes(10)
        );
        broadcast.setNewEpisode(true);

        Channel channel = Channel.builder(Publisher.BBC).build();
        channel.setId(1234L);

        ResolvedChannel resolvedChannel = ResolvedChannel.builder(channel).build();

        resolvedBroadcast = ResolvedBroadcast.create(broadcast, resolvedChannel);

        when(context.getRequest()).thenReturn(request);
        when(request.getParameter(annotations)).thenReturn("");
    }

    @Test
    public void writeNewEpisode() throws Exception {
        broadcastWriter.write(resolvedBroadcast,  writer, context);

        verify(writer).writeField("new_episode", true);
    }
}
