package org.atlasapi.query.v4.schedule;

import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;

import java.io.IOException;

import javax.servlet.http.HttpServletRequest;

import org.atlasapi.channel.Channel;
import org.atlasapi.channel.ChannelResolver;
import org.atlasapi.content.Broadcast;
import org.atlasapi.content.Content;
import org.atlasapi.content.Episode;
import org.atlasapi.content.Item;
import org.atlasapi.content.ItemAndBroadcast;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.output.AnnotationRegistry;
import org.atlasapi.output.EntityListWriter;
import org.atlasapi.output.EntityWriter;
import org.atlasapi.output.JsonResponseWriter;
import org.atlasapi.output.License;
import org.atlasapi.output.writers.BroadcastWriter;
import org.atlasapi.output.writers.LicenseWriter;
import org.atlasapi.output.writers.RequestWriter;
import org.atlasapi.persistence.output.ContainerSummaryResolver;
import org.atlasapi.query.common.QueryContext;
import org.atlasapi.query.common.QueryResult;
import org.atlasapi.query.v4.channel.ChannelListWriter;
import org.atlasapi.schedule.ChannelSchedule;
import org.atlasapi.system.legacy.NoOpChannelResolver;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.junit.Test;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.metabroadcast.common.ids.SubstitutionTableNumberCodec;
import com.metabroadcast.common.servlet.StubHttpServletRequest;
import com.metabroadcast.common.servlet.StubHttpServletResponse;
import com.metabroadcast.common.time.DateTimeZones;

public class ScheduleQueryResultWriterTest {

    private final ContainerSummaryResolver containerSummaryResolver = mock(ContainerSummaryResolver.class);
    private final AnnotationRegistry<Content> contentAnnotations = AnnotationRegistry.<Content>builder().build();
    private final AnnotationRegistry<Channel> channelAnnotations = AnnotationRegistry.<Channel>builder().build();
    private EntityWriter<Content> contentWriter = new ContentListWriter(contentAnnotations);
    private EntityWriter<Broadcast> broadcastWriter = new BroadcastWriter("broadcasts", SubstitutionTableNumberCodec.lowerCaseOnly(), new NoOpChannelResolver());
    private final EntityListWriter<ChannelSchedule> scheduleWriter
        = new ScheduleListWriter(new ChannelListWriter(channelAnnotations), new ScheduleEntryListWriter(contentWriter, broadcastWriter));
    private final EntityWriter<Object> licenseWriter = new LicenseWriter(new License("test"));
    private final ScheduleQueryResultWriter writer = new ScheduleQueryResultWriter(scheduleWriter, licenseWriter, new RequestWriter());
      
    
    @Test
    public void testWrite() throws IOException {
        Channel channel = Channel.builder(Publisher.BBC).build();
        channel.setId(1234l);
        
        DateTime from = new DateTime(0, DateTimeZones.UTC);
        DateTime to = new DateTime(1000, DateTimeZones.UTC);
        Interval interval = new Interval(from, to);
        
        Item item = new Item("aUri","aCurie",Publisher.BBC);
        item.setId(4321l);
        item.setTitle("aTitle");
        Broadcast broadcast = new Broadcast(channel, from, to);
        ItemAndBroadcast itemAndBroadcast = new ItemAndBroadcast(item, broadcast);
        
        Item episode = new Episode("bUri", "bCurie", Publisher.BBC);
        episode.setId(4322l);
        Broadcast broadcast2 = new Broadcast(channel, to, to.plusSeconds(2));
        ItemAndBroadcast episodeAndBroadcast = new ItemAndBroadcast(episode , broadcast2);
        
        Iterable<ItemAndBroadcast> entries = ImmutableList.of(itemAndBroadcast, episodeAndBroadcast);
        ChannelSchedule cs = new ChannelSchedule(channel, interval, entries);
        
        HttpServletRequest request = new StubHttpServletRequest();
        StubHttpServletResponse response = new StubHttpServletResponse();
        JsonResponseWriter responseWriter = new JsonResponseWriter(request, response);
        QueryContext context = QueryContext.standard(mock(HttpServletRequest.class));
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
