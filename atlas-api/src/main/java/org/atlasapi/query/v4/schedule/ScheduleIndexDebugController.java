package org.atlasapi.query.v4.schedule;

import java.util.concurrent.TimeUnit;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.atlasapi.application.ApplicationFetcher;
import org.atlasapi.media.channel.Channel;
import org.atlasapi.media.channel.ChannelResolver;
import org.atlasapi.query.annotation.ActiveAnnotations;
import org.atlasapi.query.annotation.ContextualAnnotationsExtractor;
import org.atlasapi.query.common.exceptions.InvalidAnnotationException;
import org.atlasapi.schedule.ScheduleIndex;
import org.atlasapi.schedule.ScheduleRef;

import com.metabroadcast.common.time.SystemClock;

import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSerializer;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.joda.time.Interval;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;

@Controller
public class ScheduleIndexDebugController {

    private static final Duration MAX_REQUEST_DURATION = Duration.standardDays(1);

    private final Gson gson = new GsonBuilder()
            .registerTypeAdapter(
                    DateTime.class,
                    (JsonSerializer<DateTime>) (src, typeOfSrc, context) ->
                            new JsonPrimitive(src.toString())
            )
            .create();
    private final ScheduleIndex index;
    private final ScheduleRequestParser requestParser;

    private final ChannelResolver channelResolver;

    public ScheduleIndexDebugController(
            ScheduleIndex index,
            ChannelResolver channelResolver,
            ApplicationFetcher appFetcher
    ) {
        this.index = index;
        this.channelResolver = channelResolver;
        this.requestParser = new ScheduleRequestParser(
                appFetcher,
                MAX_REQUEST_DURATION,
                new SystemClock(), new ContextualAnnotationsExtractor() {

            @Override
            public ActiveAnnotations extractFromRequest(HttpServletRequest request)
                    throws InvalidAnnotationException {
                return ActiveAnnotations.standard();
            }

            @Override
            public ImmutableSet<String> getParameterNames() {
                return ImmutableSet.of("annotations");
            }

        }
        );
    }

    @RequestMapping({ "/system/debug/schedules/{cid}\\.[a-z]+", "/system/debug/schedules/{cid}" })
    public void debugSchedule(HttpServletRequest request, HttpServletResponse response)
            throws Exception {
        ScheduleQuery query = requestParser.queryFrom(request);
        Channel channel = channelResolver.fromId(query.getChannelId().longValue()).requireValue();
        ListenableFuture<ScheduleRef> resolveSchedule = index.resolveSchedule(
                query.getSource(),
                channel,
                new Interval(query.getStart(), query.getEnd().get()
                )
        );
        gson.toJson(resolveSchedule.get(5, TimeUnit.SECONDS), response.getWriter());
    }

}
