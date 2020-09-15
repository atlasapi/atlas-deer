package org.atlasapi.query.v4.schedule;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import com.metabroadcast.applications.client.model.internal.Application;
import com.metabroadcast.common.ids.SubstitutionTableNumberCodec;
import com.metabroadcast.common.servlet.StubHttpServletRequest;
import com.metabroadcast.common.servlet.StubHttpServletResponse;
import com.metabroadcast.common.time.DateTimeZones;
import org.atlasapi.channel.Channel;
import org.atlasapi.channel.ChannelResolver;
import org.atlasapi.channel.ResolvedChannel;
import org.atlasapi.content.Broadcast;
import org.atlasapi.content.Content;
import org.atlasapi.content.Episode;
import org.atlasapi.content.Item;
import org.atlasapi.content.ItemAndBroadcast;
import org.atlasapi.content.ResolvedBroadcast;
import org.atlasapi.entity.util.Resolved;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.output.AnnotationRegistry;
import org.atlasapi.output.EntityListWriter;
import org.atlasapi.output.EntityWriter;
import org.atlasapi.output.JsonResponseWriter;
import org.atlasapi.output.License;
import org.atlasapi.output.ResolvedChannelResolver;
import org.atlasapi.output.writers.BroadcastWriter;
import org.atlasapi.output.writers.LicenseWriter;
import org.atlasapi.output.writers.RequestWriter;
import org.atlasapi.query.annotation.ActiveAnnotations;
import org.atlasapi.query.common.QueryResult;
import org.atlasapi.query.common.context.QueryContext;
import org.atlasapi.query.v4.channel.ChannelListWriter;
import org.atlasapi.schedule.ChannelSchedule;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.junit.Before;
import org.junit.Test;

import javax.servlet.http.HttpServletRequest;
import java.io.IOException;

import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ScheduleQueryResultWriterTest {

    private final ChannelResolver channelResolver = mock(ChannelResolver.class);
    private final ResolvedChannelResolver resolvedChannelResolver = new ResolvedChannelResolver(channelResolver);

    private final AnnotationRegistry<Content> contentAnnotations = AnnotationRegistry.<Content>builder()
            .build();
    private final AnnotationRegistry<ResolvedChannel> channelAnnotations = AnnotationRegistry.<ResolvedChannel>builder()
            .build();
    private EntityWriter<Content> contentWriter = new ContentListWriter(contentAnnotations);
    private EntityWriter<ResolvedBroadcast> broadcastWriter = BroadcastWriter.create(
            "broadcasts",
            "broadcast",
            SubstitutionTableNumberCodec.lowerCaseOnly()
    );
    private final EntityListWriter<ChannelSchedule> scheduleWriter = new ScheduleListWriter(
            new ChannelListWriter(channelAnnotations),
            new ScheduleEntryListWriter(contentWriter, broadcastWriter, resolvedChannelResolver),
            channelResolver
    );
    private final EntityWriter<Object> licenseWriter = new LicenseWriter(new License("test"));
    private final ScheduleQueryResultWriter writer = new ScheduleQueryResultWriter(
            scheduleWriter,
            licenseWriter,
            new RequestWriter()
    );

    private Channel channel;

    @Before
    public void setUp() throws Exception {

        ListenableFuture<Resolved<Channel>> listenableFuture = mock(ListenableFuture.class);

        channel = Channel.builder(Publisher.BBC).build();
        channel.setId(1234L);

        when(listenableFuture.get()).thenReturn(Resolved.valueOf(ImmutableList.of(channel)));
        when(channelResolver.resolveIds(any())).thenReturn(listenableFuture);
    }

    @Test
    public void testWrite() throws IOException {
        DateTime from = new DateTime(0, DateTimeZones.UTC);
        DateTime to = new DateTime(1000, DateTimeZones.UTC);
        Interval interval = new Interval(from, to);

        Item item = new Item("aUri", "aCurie", Publisher.BBC);
        item.setId(4321L);
        item.setTitle("aTitle");
        Broadcast broadcast = new Broadcast(channel, from, to);
        ItemAndBroadcast itemAndBroadcast = new ItemAndBroadcast(item, broadcast);

        Item episode = new Episode("bUri", "bCurie", Publisher.BBC);
        episode.setId(4322L);
        Broadcast broadcast2 = new Broadcast(channel, to, to.plusSeconds(2));
        ItemAndBroadcast episodeAndBroadcast = new ItemAndBroadcast(episode, broadcast2);

        Iterable<ItemAndBroadcast> entries = ImmutableList.of(
                itemAndBroadcast,
                episodeAndBroadcast
        );
        ChannelSchedule cs = new ChannelSchedule(channel, interval, entries);

        HttpServletRequest request = new StubHttpServletRequest();
        StubHttpServletResponse response = new StubHttpServletResponse();
        JsonResponseWriter responseWriter = new JsonResponseWriter(request, response);
        QueryContext context = QueryContext.create(
                mock(Application.class),
                ActiveAnnotations.standard(),
                mock(HttpServletRequest.class)
        );
        QueryResult<ChannelSchedule> result = QueryResult.singleResult(cs, context);

        writer.write(result, responseWriter);

        response.getWriter().flush();
        ObjectMapper mapper = new ObjectMapper();
        //TODO match the expected values
        String responseAsString = response.getResponseAsString();

        assertThat(responseAsString, containsString("\"broadcast_on\":\"cyz\""));

        //        System.out.println(
        //                mapper.writerWithDefaultPrettyPrinter().writeValueAsString(mapper.readValue(responseAsString,Object.class)));
    }

}
