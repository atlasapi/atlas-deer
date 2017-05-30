package org.atlasapi.query.v4.channel;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import javax.servlet.http.HttpServletRequest;

import org.atlasapi.channel.Channel;
import org.atlasapi.channel.ChannelGroupResolver;
import org.atlasapi.channel.ChannelType;
import org.atlasapi.channel.ResolvedChannel;
import org.atlasapi.content.MediaType;
import org.atlasapi.content.RelatedLink;
import org.atlasapi.entity.Alias;
import org.atlasapi.media.channel.TemporalField;
import org.atlasapi.media.entity.Image;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.output.ChannelGroupSummaryWriter;
import org.atlasapi.output.EntityListWriter;
import org.atlasapi.output.FieldWriter;
import org.atlasapi.output.OutputContext;
import org.atlasapi.output.writers.ImageListWriter;
import org.atlasapi.output.writers.SourceWriter;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import org.joda.time.DateTime;
import org.joda.time.LocalDate;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.times;
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
    private String advertisedTo = "advertised_to";
    private String shortDescription = "short_description";
    private String mediumDescription = "medium_description";
    private String longDescription = "long_description";
    private String region = "region";
    private String targetRegions = "target_regions";
    private String targetRegion = "target_region";
    private String channelType = "channel_type";
    private String genres = "genres";
    private TemporalField temporalField = new TemporalField(title, null, null);
    private MediaType mediaTypeObject = MediaType.VIDEO;
    private boolean highDefinitionValue = true;
    private boolean regionalValue = true;
    private LocalDate startDateValue = LocalDate.parse("2016-05-26");
    private DateTime advertisedFromValue = DateTime.parse("2016-05-27T07:15:30.450Z");
    private DateTime advertisedToValue = DateTime.parse("2017-05-27T07:15:30.450Z");
    private String interactive = "interactive";

    @Before
    public void setUp() throws Exception {

        this.channelWriter = ChannelWriter.create(
                listName,
                fieldName,
                channelGroupSummaryWriter
        );
    }

    @Test
    public void noFieldsHasBeenSet() throws IOException {
        this.channel = Channel.builder(Publisher.METABROADCAST)
                .withId(199999L) // ID always needs to be specified, otherwise getting NPE.
                .build();

        when(outputContext.getRequest()).thenReturn(request);
        when(request.getParameter(annotations)).thenReturn("");

        channelWriter.write(ResolvedChannel.builder(channel).build(), fieldWriter, outputContext);

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
        verify(fieldWriter).writeField(eq(channelType), eq(ChannelType.CHANNEL));
        verify(fieldWriter).writeField(eq(interactive), eq(false));
        verify(fieldWriter).writeList(
                eq(genres),
                eq(genres),
                eq(Sets.newHashSet()),
                any(OutputContext.class)
        );
        verify(fieldWriter).writeList(
                eq(targetRegions),
                eq(targetRegion),
                eq(Sets.newHashSet()),
                any(OutputContext.class)
        );
        verify(fieldWriter, times(4)).writeList(
                any(EntityListWriter.class),
                eq(Sets.newHashSet()),
                any(OutputContext.class)
        );
    }

    @Test
    public void allFieldsHasBeenSet() throws IOException {
        Set<String> genreSet = Sets.newHashSet("genre 1", "genre 2");
        Set<String> targetRegionsSet = Sets.newHashSet("target region 1", "target region 2");
        HashSet iamgeSet = Sets.newHashSet(
                new TemporalField(
                        new Image("image 1"), null, null
                ),
                new TemporalField(
                        new Image("image 1"), null, null
                )
        );
        Set<Alias> aliasSet = Sets.newHashSet(
                new Alias("alias 1", "value 1"),
                new Alias("alias 2", "value 2")
        );
        Set<RelatedLink> relatedLinkSet = Sets.newHashSet(
                RelatedLink.unknownTypeLink("test 1").build(),
                RelatedLink.unknownTypeLink("test 2").build()
        );
        Set<Publisher> availabilitySet = Sets.newHashSet(Publisher.PA, Publisher.INTERNET_VIDEO_ARCHIVE);
        this.channel = Channel.builder(Publisher.METABROADCAST)
                .withTitles(ImmutableList.of(temporalField))
                .withId(199999L)
                .withUri(uri)
                .withMediaType(mediaTypeObject)
                .withHighDefinition(highDefinitionValue)
                .withRegional(regionalValue)
                .withStartDate(startDateValue)
                .withAdvertiseFrom(advertisedFromValue)
                .withAdvertiseTo(advertisedToValue)
                .withShortDescription(shortDescription)
                .withMediumDescription(mediumDescription)
                .withLongDescription(longDescription)
                .withRegion(region)
                .withChannelType(ChannelType.CHANNEL)
                .withGenres(genreSet)
                .withTargetRegions(targetRegionsSet)
                .withImages(iamgeSet)
                .withAvailableFrom(availabilitySet)
                .withAliases(aliasSet)
                .withRelatedLinks(relatedLinkSet)
                .withInteractive(true)
                .build();

        when(outputContext.getRequest()).thenReturn(request);
        when(request.getParameter(annotations)).thenReturn("");

        channelWriter.write(ResolvedChannel.builder(channel).build(), fieldWriter, outputContext);

        verify(fieldWriter).writeField(eq(title), eq(title));
        verify(fieldWriter).writeField(eq(id), eq("pgnp"));
        verify(fieldWriter).writeField(eq(uri), eq(uri));
        verify(fieldWriter).writeField(eq(mediaType), eq(mediaTypeObject));
        verify(fieldWriter).writeField(eq(highDefinition), eq(highDefinitionValue));
        verify(fieldWriter).writeField(eq(regional), eq(regionalValue));
        verify(fieldWriter).writeField(eq(startDate), eq(startDateValue));
        verify(fieldWriter).writeField(eq(advertisedFrom), eq(advertisedFromValue));
        verify(fieldWriter).writeField(eq(advertisedTo), eq(advertisedToValue));
        verify(fieldWriter).writeField(eq(shortDescription), eq(shortDescription));
        verify(fieldWriter).writeField(eq(mediumDescription), eq(mediumDescription));
        verify(fieldWriter).writeField(eq(longDescription), eq(longDescription));
        verify(fieldWriter).writeField(eq(region), eq(region));
        verify(fieldWriter).writeField(eq(channelType), eq(ChannelType.CHANNEL));
        verify(fieldWriter).writeField(eq(interactive),  eq(true));
        verify(fieldWriter).writeList(
                eq(genres),
                eq(genres),
                eq(genreSet),
                any(OutputContext.class)
        );
        verify(fieldWriter).writeList(
                eq(targetRegions),
                eq(targetRegion),
                eq(targetRegionsSet),
                any(OutputContext.class)
        );
        verify(fieldWriter, times(4)).writeList(
                any(ImageListWriter.class),
                any(Iterable.class),
                any(OutputContext.class)
        );
        verify(fieldWriter).writeObject(
                any(SourceWriter.class),
                eq(Publisher.METABROADCAST),
                any(OutputContext.class)
        );
    }
}