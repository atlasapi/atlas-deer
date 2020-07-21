package org.atlasapi.system.debug;

import java.io.IOException;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import javax.servlet.http.HttpServletResponse;

import org.atlasapi.channel.Channel;
import org.atlasapi.channel.ChannelResolver;
import org.atlasapi.entity.Id;
import org.atlasapi.entity.util.Resolved;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.schedule.EquivalentSchedule;
import org.atlasapi.schedule.EquivalentScheduleStore;
import org.atlasapi.schedule.Schedule;
import org.atlasapi.schedule.ScheduleStore;

import com.metabroadcast.common.ids.NumberToShortStringCodec;
import com.metabroadcast.common.ids.SubstitutionTableNumberCodec;

import com.google.common.base.Optional;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSerializer;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.joda.time.LocalDate;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;

@Controller
public class ScheduleDebugController {

    private final NumberToShortStringCodec lowercase = SubstitutionTableNumberCodec.lowerCaseOnly();
    private static final DateTimeFormatter dateParser = ISODateTimeFormat.date();
    private final ChannelResolver channelResolver;
    private final EquivalentScheduleStore equivalentScheduleStore;
    private final ScheduleStore scheduleStore;
    private final Gson gson;

    public ScheduleDebugController(
            ChannelResolver channelResolver,
            EquivalentScheduleStore equivalentScheduleStore,
            ScheduleStore scheduleStore) {
        this.channelResolver = channelResolver;
        this.equivalentScheduleStore = equivalentScheduleStore;
        this.scheduleStore = scheduleStore;
        this.gson = new GsonBuilder()
                .registerTypeAdapter(
                        DateTime.class,
                        (JsonSerializer<DateTime>) (src, typeOfSrc, context) ->
                                new JsonPrimitive(src.toString())
                )
                .registerTypeAdapter(
                        Interval.class,
                        (JsonSerializer<Interval>) (src, typeOfSrc, context) ->
                                new JsonPrimitive(src.toString())
                )
                .create();
    }

    @RequestMapping("/system/debug/schedule")
    public void debugEquivalentSchedule(
            @RequestParam(value = "channel", required = true) String channelId,
            @RequestParam(value = "source", required = true) String sourceKey,
            @RequestParam(value = "selectedSources", required = false) String selectedSourcesKeys,
            @RequestParam(value = "day", required = true) String day,
            final HttpServletResponse response
    ) throws IOException {
        try {

            Publisher source = Publisher.fromKey(sourceKey).requireValue();

            Set<Publisher> selectedSources = null;

            if (selectedSourcesKeys != null) {
                selectedSources = Splitter.on(",")
                        .splitToList(selectedSourcesKeys)
                        .stream()
                        .map(key -> Publisher.fromKey(key).requireValue())
                        .collect(Collectors.toSet());
            } else {
                selectedSources = Publisher.all();
            }

            Optional<LocalDate> opDate = parseDate(day);
            if (!opDate.isPresent()) {
                response.setStatus(400);
                response.getWriter().write("Failed to parse " + day + ", expected yyyy-MM-dd");
                return;
            }

            LocalDate date = opDate.get();

            Optional<Channel> channel = resolve(channelId);
            if (!channel.isPresent()) {
                response.setStatus(404);
                response.getWriter().write("Unknown channel " + channelId);
            }

            EquivalentSchedule equivalentSchedule = Futures.get(equivalentScheduleStore.resolveSchedules(
                    ImmutableSet.of(channel.get()),
                    new Interval(
                            date.toDateTimeAtStartOfDay(),
                            date.plusDays(1).toDateTimeAtStartOfDay()
                    ),
                    source,
                    selectedSources
            ), Exception.class);

            gson.toJson(equivalentSchedule, response.getWriter());
            response.setStatus(200);
            response.flushBuffer();
        } catch (Throwable t) {
            t.printStackTrace(response.getWriter());
        }
    }

    @RequestMapping("/system/debug/schedule/unequivalated")
    public void debugScheduleStore(
            @RequestParam(value = "channel", required = true) String channelId,
            @RequestParam(value = "source", required = true) String sourceKey,
            @RequestParam(value = "day", required = true) String day,
            final HttpServletResponse response) throws IOException {

        try {

            Publisher source = Publisher.fromKey(sourceKey).requireValue();

            Optional<LocalDate> opDate = parseDate(day);
            if (!opDate.isPresent()) {
                response.setStatus(400);
                response.getWriter().write("Failed to parse " + day + ", expected yyyy-MM-dd");
                return;
            }

            LocalDate date = opDate.get();

            Optional<Channel> channel = resolve(channelId);
            if (!channel.isPresent()) {
                response.setStatus(404);
                response.getWriter().write("Unknown channel " + channelId);
            }

            Schedule schedule = Futures.get(scheduleStore.resolve(
                    ImmutableSet.of(channel.get()),
                    new Interval(
                            date.toDateTimeAtStartOfDay(),
                            date.plusDays(1).toDateTimeAtStartOfDay()
                    ),
                    source
            ), Exception.class);

            gson.toJson(schedule, response.getWriter());
            response.setStatus(200);
            response.flushBuffer();
        } catch (Throwable t) {
            t.printStackTrace(response.getWriter());
        }
    }

    private Optional<Channel> resolve(String channelId) throws Exception {
        Id cid = Id.valueOf(lowercase.decode(channelId));
        ListenableFuture<Resolved<Channel>> channelFuture = channelResolver.resolveIds(ImmutableList
                .of(cid));
        Resolved<Channel> resolvedChannel = Futures.get(
                channelFuture,
                1,
                TimeUnit.MINUTES,
                Exception.class
        );

        if (resolvedChannel.getResources().isEmpty()) {
            return Optional.absent();
        }
        return Optional.of(Iterables.getOnlyElement(resolvedChannel.getResources()));
    }

    private Optional<LocalDate> parseDate(String day) {
        try {
            return Optional.of(dateParser.parseLocalDate(day));
        } catch (IllegalArgumentException exception) {
            return Optional.absent();
        }
    }

}
