package org.atlasapi.system.bootstrap;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.metabroadcast.common.http.HttpStatusCode.BAD_REQUEST;
import static com.metabroadcast.common.http.HttpStatusCode.SERVER_ERROR;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ExecutorService;

import javax.servlet.http.HttpServletResponse;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Maps;
import org.atlasapi.media.channel.Channel;
import org.atlasapi.media.channel.ChannelResolver;
import org.atlasapi.media.entity.Publisher;
import org.elasticsearch.common.base.Throwables;
import org.joda.time.Interval;
import org.joda.time.LocalDate;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;

import com.metabroadcast.common.base.Maybe;
import com.metabroadcast.common.http.HttpStatusCode;
import com.metabroadcast.common.ids.NumberToShortStringCodec;
import com.metabroadcast.common.ids.SubstitutionTableNumberCodec;
import com.metabroadcast.common.scheduling.UpdateProgress;
import com.metabroadcast.common.time.DateTimeZones;

@Controller
public class ScheduleBootstrapController {
    
    private final ChannelIntervalScheduleBootstrapTaskFactory taskFactory;
    private final ChannelResolver channelResvoler;
    private final ExecutorService executor;
    private final ScheduleBootstrapper scheduleBootstrapper;
    private final ObjectMapper jsonMapper = new ObjectMapper();

    private static final DateTimeFormatter dateParser = ISODateTimeFormat.date();
    private static final NumberToShortStringCodec idCodec = SubstitutionTableNumberCodec.lowerCaseOnly();
    private static final Logger log = LoggerFactory.getLogger(ScheduleBootstrapper.class);

    public ScheduleBootstrapController(
            ChannelIntervalScheduleBootstrapTaskFactory taskFactory,
            ChannelResolver channelResvoler,
            ExecutorService executor,
            ScheduleBootstrapper scheduleBootstrapper
    ) {
        this.executor = checkNotNull(executor);
        this.scheduleBootstrapper = checkNotNull(scheduleBootstrapper);
        this.taskFactory = checkNotNull(taskFactory);
        this.channelResvoler = checkNotNull(channelResvoler);
    }
    
    @RequestMapping(value="/system/bootstrap/schedule",method=RequestMethod.POST)
    public Void bootstrapSchedule(HttpServletResponse resp, @RequestParam("source") String src,
            @RequestParam("day") String day, @RequestParam("channel") String channelId)
        throws IOException {
        
        Maybe<Publisher> source = Publisher.fromKey(src);
        if (!source.hasValue()) {
            return failure(resp, BAD_REQUEST, "Unknown source " + src);
        }
        
        Maybe<Channel> channel = resolve(channelId);
        if (!channel.hasValue()) {
            return failure(resp, BAD_REQUEST, "Unknown channel " + channelId);
        }
        
        LocalDate date;
        try {
            date = dateParser.parseLocalDate(day);
        } catch (IllegalArgumentException iae) {
            return failure(resp, BAD_REQUEST, "Failed to parse "+day+", expected yyyy-MM-dd");
        }
        
        try {
            UpdateProgress progress = taskFactory.create(source.requireValue(), channel.requireValue(), interval(date)).call();
            resp.setStatus(HttpStatusCode.OK.code());
            resp.getWriter().write(progress.toString());
            return null;
        } catch (Exception e) {
            return failure(resp, SERVER_ERROR, Throwables.getStackTraceAsString(e));
        }
    }

    @RequestMapping(value="/system/bootstrap/schedule/all",method=RequestMethod.POST)
    public void bootstrapAllSchedules(HttpServletResponse resp, @RequestParam("source") String src,
                                  @RequestParam("from") String from, @RequestParam("to") String to)
            throws IOException {

        final Maybe<Publisher> source = Publisher.fromKey(src);
        if (!source.hasValue()) {
            failure(resp, BAD_REQUEST, "Unknown source " + src);
            return;
        }

        final Iterable<Channel> channels = channelResvoler.all();

        final LocalDate dateFrom;
        final LocalDate dateTo;
        try {
            dateFrom = dateParser.parseLocalDate(from);
        } catch (IllegalArgumentException iae) {
            failure(resp, BAD_REQUEST, "Failed to parse "+ from +", expected yyyy-MM-dd");
            return;
        }

        try {
            dateTo = dateParser.parseLocalDate(to);
        } catch (IllegalArgumentException iae) {
            failure(resp, BAD_REQUEST, "Failed to parse "+ to +", expected yyyy-MM-dd");
            return;
        }
        final Interval interval = interval(dateFrom, dateTo);
        executor.submit(new Runnable() {
            @Override
            public void run() {
                boolean bootstrapping = scheduleBootstrapper.bootstrapSchedules(
                        channels,
                        interval,
                        source.requireValue()
                );
                if (!bootstrapping) {
                    log.warn("Bootstrapping failed because apparently busy bootstrapping something else.");
                }
            }
        });
    }

    @RequestMapping(value="/system/bootstrap/schedule/all/status.json",method=RequestMethod.GET)
    public void checkScheduleBootstrapStatus(HttpServletResponse response) throws IOException {
        Map<String, Object> result = Maps.newHashMap();
        result.put("bootstrapping", scheduleBootstrapper.isBootstrapping());
        result.put("processed", scheduleBootstrapper.getProgress().getProcessed());
        result.put("failures", scheduleBootstrapper.getProgress().getFailures());
        jsonMapper.writeValue(response.getOutputStream(), result);
        response.flushBuffer();
    }

    private Interval interval(LocalDate from, LocalDate to) {
        return new Interval(
                from.toDateTimeAtStartOfDay(DateTimeZones.UTC),
                to.toDateTimeAtStartOfDay(DateTimeZones.UTC)
        );
    }
    private Interval interval(LocalDate day) {
        return new Interval(day.toDateTimeAtStartOfDay(DateTimeZones.UTC),
                day.plusDays(1).toDateTimeAtStartOfDay(DateTimeZones.UTC));
    }

    private Maybe<Channel> resolve(String channelId) {
        return channelResvoler.fromId(idCodec.decode(channelId).longValue());
    }

    private Void failure(HttpServletResponse resp, HttpStatusCode status, String msg) throws IOException {
        resp.setStatus(status.code());
        resp.getWriter().write(msg);
        return null;
    }
    
}
