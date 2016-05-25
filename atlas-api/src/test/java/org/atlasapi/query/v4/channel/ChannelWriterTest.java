package org.atlasapi.query.v4.channel;

import java.io.IOException;
import java.util.Set;

import javax.servlet.http.HttpServletRequest;

import org.atlasapi.channel.Channel;
import org.atlasapi.channel.ChannelGroupResolver;
import org.atlasapi.channel.ChannelType;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.output.ChannelGroupSummaryWriter;
import org.atlasapi.output.FieldWriter;
import org.atlasapi.output.OutputContext;

import com.google.common.collect.Sets;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class ChannelWriterTest {

    @Mock ChannelGroupResolver channelGroupResolver;
    @Mock ChannelGroupSummaryWriter channelGroupSummaryWriter;
    @Mock FieldWriter fieldWriter;
    @Mock OutputContext outputContext;
    @Mock HttpServletRequest request;
    private ChannelWriter channelWriter;
    private Channel channel;
    private String listName = "name";
    private String fieldName = "field";
    private String annotations = "annotations";
    private String title = "title";
    private String id = "id";
    private String uri = "uri";
    private String mediaType = "media_type";
    private String highDefinition = "high_definition";
    private String regional = "regional";
    private String startDate = "start_date";
    private String advertisedFrom = "advertised_from";
    private String shortDescription = "short_description";
    private String mediumDescription = "medium_description";
    private String longDescription = "long_description";
    private String region = "region";
    private String targetRegions = "target_regions";
    private String channelType = "channel_type";

    @Before
    public void setUp() throws Exception {

        this.channelWriter = new ChannelWriter(
                channelGroupResolver,
                listName,
                fieldName,
                channelGroupSummaryWriter
        );
    }

    @Test
    public void writingChannelShortDescription() throws IOException {
        this.channel = Channel.builder(Publisher.METABROADCAST)
                .withId(199999L)
                .withShortDescription(shortDescription)
                .build();

        ArgumentCaptor<String> argumentCaptor = ArgumentCaptor.forClass(String.class);

        when(outputContext.getRequest()).thenReturn(request);
        when(request.getParameter(annotations)).thenReturn("");

        channelWriter.write(channel, fieldWriter, outputContext);

        verify(fieldWriter).writeField(eq(title), eq(null));
        verify(fieldWriter).writeField(eq(id), eq("pgnp"));
        verify(fieldWriter).writeField(eq(uri), eq(null));
        verify(fieldWriter).writeField(eq(mediaType), eq(null));
        verify(fieldWriter).writeField(eq(highDefinition), eq(null));
        verify(fieldWriter).writeField(eq(regional), eq(null));
        verify(fieldWriter).writeField(eq(startDate), eq(null));
        verify(fieldWriter).writeField(eq(advertisedFrom), eq(null));
        verify(fieldWriter).writeField(eq(shortDescription), argumentCaptor.capture());
        verify(fieldWriter).writeField(eq(shortDescription), eq(shortDescription));
    }

    @Test
    public void writingChannelMediumDescription() throws IOException {
        this.channel = Channel.builder(Publisher.METABROADCAST)
                .withId(199999L)
                .withMediumDescription(mediumDescription)
                .build();

        ArgumentCaptor<String> argumentCaptor = ArgumentCaptor.forClass(String.class);

        when(outputContext.getRequest()).thenReturn(request);
        when(request.getParameter(annotations)).thenReturn("");

        channelWriter.write(channel, fieldWriter, outputContext);

        verify(fieldWriter).writeField(eq(title), eq(null));
        verify(fieldWriter).writeField(eq(id), eq("pgnp"));
        verify(fieldWriter).writeField(eq(uri), eq(null));
        verify(fieldWriter).writeField(eq(mediaType), eq(null));
        verify(fieldWriter).writeField(eq(highDefinition), eq(null));
        verify(fieldWriter).writeField(eq(regional), eq(null));
        verify(fieldWriter).writeField(eq(startDate), eq(null));
        verify(fieldWriter).writeField(eq(advertisedFrom), eq(null));
        verify(fieldWriter).writeField(eq(shortDescription), eq(null));
        verify(fieldWriter).writeField(eq(mediumDescription), argumentCaptor.capture());
        verify(fieldWriter).writeField(eq(mediumDescription), eq(mediumDescription));
    }

    @Test
    public void writingChannelLongDescription() throws IOException {
        this.channel = Channel.builder(Publisher.METABROADCAST)
                .withId(199999L)
                .withLongDescription(longDescription)
                .build();

        ArgumentCaptor<String> argumentCaptor = ArgumentCaptor.forClass(String.class);

        when(outputContext.getRequest()).thenReturn(request);
        when(request.getParameter(annotations)).thenReturn("");

        channelWriter.write(channel, fieldWriter, outputContext);

        verify(fieldWriter).writeField(eq(title), eq(null));
        verify(fieldWriter).writeField(eq(id), eq("pgnp"));
        verify(fieldWriter).writeField(eq(uri), eq(null));
        verify(fieldWriter).writeField(eq(mediaType), eq(null));
        verify(fieldWriter).writeField(eq(highDefinition), eq(null));
        verify(fieldWriter).writeField(eq(regional), eq(null));
        verify(fieldWriter).writeField(eq(startDate), eq(null));
        verify(fieldWriter).writeField(eq(advertisedFrom), eq(null));
        verify(fieldWriter).writeField(eq(shortDescription), eq(null));
        verify(fieldWriter).writeField(eq(mediumDescription), eq(null));
        verify(fieldWriter).writeField(eq(longDescription), argumentCaptor.capture());
        verify(fieldWriter).writeField(eq(longDescription), eq(longDescription));
    }

    @Test
    public void writingChannelRegion() throws IOException {
        this.channel = Channel.builder(Publisher.METABROADCAST)
                .withId(199999L)
                .withRegion(region)
                .build();

        ArgumentCaptor<String> argumentCaptor = ArgumentCaptor.forClass(String.class);

        when(outputContext.getRequest()).thenReturn(request);
        when(request.getParameter(annotations)).thenReturn("");

        channelWriter.write(channel, fieldWriter, outputContext);

        verify(fieldWriter).writeField(eq(title), eq(null));
        verify(fieldWriter).writeField(eq(id), eq("pgnp"));
        verify(fieldWriter).writeField(eq(uri), eq(null));
        verify(fieldWriter).writeField(eq(mediaType), eq(null));
        verify(fieldWriter).writeField(eq(highDefinition), eq(null));
        verify(fieldWriter).writeField(eq(regional), eq(null));
        verify(fieldWriter).writeField(eq(startDate), eq(null));
        verify(fieldWriter).writeField(eq(advertisedFrom), eq(null));
        verify(fieldWriter).writeField(eq(shortDescription), eq(null));
        verify(fieldWriter).writeField(eq(mediumDescription), eq(null));
        verify(fieldWriter).writeField(eq(longDescription), eq(null));
        verify(fieldWriter).writeField(eq(region), argumentCaptor.capture());
        verify(fieldWriter).writeField(eq(region), eq(region));
    }

    @Test
    public void writingChannelTargetRegions() throws IOException {
        Set<String> targetRegionsSet = Sets.newHashSet("r1", "r2", "r3");

        this.channel = Channel.builder(Publisher.METABROADCAST)
                .withId(199999L)
                .withTargetRegions(targetRegionsSet)
                .build();

        when(outputContext.getRequest()).thenReturn(request);
        when(request.getParameter(annotations)).thenReturn("");

        channelWriter.write(channel, fieldWriter, outputContext);
        verify(fieldWriter).writeList(
                eq(targetRegions),
                eq(targetRegions),
                eq(targetRegionsSet),
                any(OutputContext.class)
        );
    }

    @Test
    public void writingChannelTypeOfChannel() throws IOException {
        ChannelType typeChannel = ChannelType.CHANNEL;

        this.channel = Channel.builder(Publisher.METABROADCAST)
                .withId(199999L)
                .withChannelType(typeChannel)
                .build();

        ArgumentCaptor<ChannelType> argumentCaptor = ArgumentCaptor.forClass(ChannelType.class);

        when(outputContext.getRequest()).thenReturn(request);
        when(request.getParameter(annotations)).thenReturn("");

        channelWriter.write(channel, fieldWriter, outputContext);

        verify(fieldWriter).writeField(eq(title), eq(null));
        verify(fieldWriter).writeField(eq(id), eq("pgnp"));
        verify(fieldWriter).writeField(eq(uri), eq(null));
        verify(fieldWriter).writeField(eq(mediaType), eq(null));
        verify(fieldWriter).writeField(eq(highDefinition), eq(null));
        verify(fieldWriter).writeField(eq(regional), eq(null));
        verify(fieldWriter).writeField(eq(startDate), eq(null));
        verify(fieldWriter).writeField(eq(advertisedFrom), eq(null));
        verify(fieldWriter).writeField(eq(shortDescription), eq(null));
        verify(fieldWriter).writeField(eq(mediumDescription), eq(null));
        verify(fieldWriter).writeField(eq(longDescription), eq(null));
        verify(fieldWriter).writeField(eq(region), eq(null));
        verify(fieldWriter).writeField(eq(channelType), argumentCaptor.capture());
        verify(fieldWriter).writeField(eq(channelType), eq(typeChannel));
    }

    @Test
    public void writingChannelTypeOfMasterbrand() throws IOException {
        ChannelType typeMasterbrand = ChannelType.MASTERBRAND;

        this.channel = Channel.builder(Publisher.METABROADCAST)
                .withId(199999L)
                .withChannelType(typeMasterbrand)
                .build();

        ArgumentCaptor<ChannelType> argumentCaptor = ArgumentCaptor.forClass(ChannelType.class);

        when(outputContext.getRequest()).thenReturn(request);
        when(request.getParameter(annotations)).thenReturn("");

        channelWriter.write(channel, fieldWriter, outputContext);

        verify(fieldWriter).writeField(eq(title), eq(null));
        verify(fieldWriter).writeField(eq(id), eq("pgnp"));
        verify(fieldWriter).writeField(eq(uri), eq(null));
        verify(fieldWriter).writeField(eq(mediaType), eq(null));
        verify(fieldWriter).writeField(eq(highDefinition), eq(null));
        verify(fieldWriter).writeField(eq(regional), eq(null));
        verify(fieldWriter).writeField(eq(startDate), eq(null));
        verify(fieldWriter).writeField(eq(advertisedFrom), eq(null));
        verify(fieldWriter).writeField(eq(shortDescription), eq(null));
        verify(fieldWriter).writeField(eq(mediumDescription), eq(null));
        verify(fieldWriter).writeField(eq(longDescription), eq(null));
        verify(fieldWriter).writeField(eq(region), eq(null));
        verify(fieldWriter).writeField(eq(channelType), argumentCaptor.capture());
        verify(fieldWriter).writeField(eq(channelType), eq(typeMasterbrand));
    }
}