package org.atlasapi.output.annotation;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;

import javax.servlet.http.HttpServletRequest;

import org.atlasapi.channel.Channel;
import org.atlasapi.channel.ChannelGroupResolver;
import org.atlasapi.media.channel.TemporalField;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.output.ChannelGroupSummaryWriter;
import org.atlasapi.output.FieldWriter;
import org.atlasapi.output.OutputContext;
import org.atlasapi.query.common.QueryContext;
import org.atlasapi.query.v4.channel.ChannelWriter;
import org.joda.time.DateTime;
import org.joda.time.LocalDate;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.google.common.collect.ImmutableList;

@RunWith(MockitoJUnitRunner.class)
public class ChannelAdvertisedAnnotationTest {
    private @Mock ChannelWriter channelWriter;
    private @Mock FieldWriter fieldWriter;
    private ChannelAdvertisedAnnotation channelAnnotation = new ChannelAdvertisedAnnotation(new ChannelWriter(
            mock(ChannelGroupResolver.class),"","", mock(ChannelGroupSummaryWriter.class)));
    private Channel.Builder builder;
    private OutputContext context = OutputContext.valueOf(QueryContext.standard(mock(HttpServletRequest.class)));

    @Before
    public void setUp() {
        builder = new Channel
                .Builder(Publisher.BBC)
                .withId(1l)
                .withTitles(ImmutableList.of(new TemporalField<>("", LocalDate.now(),LocalDate.now())))
                .withAdult(true)
                .withAvailableFrom(Publisher.BBC)
                .withEndDate(LocalDate.now());
    }
    @Test
    public void testChannelAnnotationWritesAdvertiseFrom() throws Exception {
        Channel channel = builder.withAdvertiseFrom(DateTime.now().plusDays(1)).build();
        channelAnnotation.write(channel, fieldWriter, context);
        verify(fieldWriter).writeField("title", channel.getTitle());
        verify(fieldWriter).writeField("advertise_from", channel.getAdvertiseFrom());
    }

    @Test
    public void testChannelAnnotationDontWriteAdvertiseFrom() throws Exception {
        Channel channel = builder.withAdvertiseFrom(DateTime.now()).build();
        channelAnnotation.write(channel, fieldWriter, context);
        verifyZeroInteractions(fieldWriter);
    }

}
