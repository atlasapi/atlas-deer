package org.atlasapi.system.bootstrap;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import javax.servlet.http.HttpServletResponse;

import org.atlasapi.channel.Channel;
import org.atlasapi.channel.ChannelResolver;
import org.atlasapi.entity.Id;
import org.atlasapi.entity.util.Resolved;
import org.atlasapi.media.channel.ChannelQuery;
import org.atlasapi.media.entity.Publisher;

import com.metabroadcast.common.base.Maybe;
import com.metabroadcast.common.http.HttpStatusCode;
import com.metabroadcast.common.ids.NumberToShortStringCodec;
import com.metabroadcast.common.ids.SubstitutionTableNumberCodec;
import com.metabroadcast.common.time.DateTimeZones;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
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

import static com.google.common.base.Preconditions.checkNotNull;
import static com.metabroadcast.common.http.HttpStatusCode.BAD_REQUEST;
import static com.metabroadcast.common.http.HttpStatusCode.SERVER_ERROR;

@Controller
public class ScheduleBootstrapController {

    private final ChannelResolver channelResolver;
    private final ExecutorService executor;
    private final ScheduleBootstrapper scheduleBootstrapper;
    private final ObjectMapper jsonMapper = new ObjectMapper();

    private static final DateTimeFormatter dateParser = ISODateTimeFormat.date();
    private static final NumberToShortStringCodec idCodec = SubstitutionTableNumberCodec.lowerCaseOnly();
    private static final Logger log = LoggerFactory.getLogger(ScheduleBootstrapper.class);

    public ScheduleBootstrapController(
            ChannelResolver channelResvoler,
            ExecutorService executor,
            ScheduleBootstrapper scheduleBootstrapper
    ) {
        this.executor = checkNotNull(executor);
        this.scheduleBootstrapper = checkNotNull(scheduleBootstrapper);
        this.channelResolver = checkNotNull(channelResvoler);
    }

    /**
     * Bootstrap a single channel for a single day.
     * <p><em>Note:</em> {@code migrateContent}, {@code writeEquivs} and {@code forwarding}
     * are mutually exclusive, and have that respective precedence.</p>
     * @param src   the source/publisher
     * @param day   the day to bootstrap
     * @param channelId     the channel id (lowercase encoded string form)
     * @param migrateContent    true to migrate content hierachy as well
     * @param writeEquivs       true to migrate content equivalences as well
     * @param forwarding        true to forward to the schedule equivalence writer as well
     */
    @RequestMapping(value = "/system/bootstrap/schedule", method = RequestMethod.POST)
    public Void bootstrapSchedule(
            HttpServletResponse resp,
            @RequestParam("source") String src,
            @RequestParam("day") String day,
            @RequestParam("channel") String channelId,
            @RequestParam(value = "migrateContent", required = false,
                    defaultValue = "false") boolean migrateContent,
            @RequestParam(value = "writeEquivalences", required = false,
                    defaultValue = "false") boolean writeEquivs,
            @RequestParam(value = "forwarding", required = false,
                    defaultValue = "false") boolean forwarding
    ) throws Exception {

        Maybe<Publisher> source = Publisher.fromKey(src);
        if (!source.hasValue()) {
            return failure(resp, BAD_REQUEST, "Unknown source " + src);
        }

        Optional<Channel> channel = resolve(channelId);
        if (!channel.isPresent()) {
            return failure(resp, BAD_REQUEST, "Unknown channel " + channelId);
        }

        LocalDate date;
        try {
            date = dateParser.parseLocalDate(day);
        } catch (IllegalArgumentException iae) {
            return failure(resp, BAD_REQUEST, "Failed to parse " + day + ", expected yyyy-MM-dd");
        }

        try {
            boolean success = scheduleBootstrapper.bootstrapSchedules(
                    ImmutableList.of(channel.get()),
                    interval(date),
                    source.requireValue(),
                    migrateContent,
                    writeEquivs,
                    forwarding
            );
            resp.setStatus((success ? HttpStatusCode.OK : HttpStatusCode.CONFLICT).code());
            resp.getWriter().write(success ?
                                   scheduleBootstrapper.getProgress().toString() :
                                   "Another schedule bootstrap is already running");
            return null;
        } catch (Exception e) {
            return failure(resp, SERVER_ERROR, Throwables.getStackTraceAsString(e));
        }
    }

    /**
     * Bootstrap all channels for a date range.
     * <p><em>Note:</em> {@code migrateContent}, {@code writeEquivs} and {@code forwarding}
     * are mutually exclusive, and have that respective precedence.</p>
     * @param src   the source/publisher
     * @param from  the date to migrate from
     * @param to    the date to migrate to
     * @param migrateContent    true to migrate content hierachy as well
     * @param writeEquivs       true to migrate content equivalences as well
     * @param forwarding        true to forward to the schedule equivalence writer as well
     */
    @RequestMapping(value = "/system/bootstrap/schedule/all", method = RequestMethod.POST)
    public void bootstrapAllSchedules(
            HttpServletResponse resp,
            @RequestParam("source") String src,
            @RequestParam("from") String from,
            @RequestParam("to") String to,
            @RequestParam(value = "migrateContent", required = false,
                    defaultValue = "false") boolean migrateContent,
            @RequestParam(value = "writeEquivalences", required = false,
                    defaultValue = "false") boolean writeEquivs,
            @RequestParam(value = "forwarding", required = false,
                    defaultValue = "false") boolean forwarding
    )
            throws Exception {

        final Maybe<Publisher> source = Publisher.fromKey(src);
        if (!source.hasValue()) {
            failure(resp, BAD_REQUEST, "Unknown source " + src);
            return;
        }

        final Iterable<Channel> channels =
                Futures.get(channelResolver.resolveChannels(ChannelQuery.builder().build()),
                        1, TimeUnit.MINUTES,
                        Exception.class
                ).getResources();

        final LocalDate dateFrom;
        final LocalDate dateTo;
        try {
            dateFrom = dateParser.parseLocalDate(from);
        } catch (IllegalArgumentException iae) {
            failure(resp, BAD_REQUEST, "Failed to parse " + from + ", expected yyyy-MM-dd");
            return;
        }

        try {
            dateTo = dateParser.parseLocalDate(to);
        } catch (IllegalArgumentException iae) {
            failure(resp, BAD_REQUEST, "Failed to parse " + to + ", expected yyyy-MM-dd");
            return;
        }
        final Interval interval = interval(dateFrom, dateTo);
        executor.submit(new Runnable() {

            @Override
            public void run() {
                boolean bootstrapping = scheduleBootstrapper.bootstrapSchedules(
                        channels,
                        interval,
                        source.requireValue(),
                        migrateContent,
                        writeEquivs,
                        forwarding
                );
                if (!bootstrapping) {
                    log.warn(
                            "Bootstrapping failed because apparently busy bootstrapping something else.");
                }
            }
        });
    }

    @RequestMapping(value = "/system/bootstrap/schedule/all/status.json",
            method = RequestMethod.GET)
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
                to.plusDays(1).toDateTimeAtStartOfDay(DateTimeZones.UTC)
        );
    }

    private Interval interval(LocalDate day) {
        return new Interval(
                day.toDateTimeAtStartOfDay(DateTimeZones.UTC),
                day.plusDays(1).toDateTimeAtStartOfDay(DateTimeZones.UTC)
        );
    }

    private Optional<Channel> resolve(String channelId) throws Exception {
        Id cid = Id.valueOf(idCodec.decode(channelId));
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

    private Void failure(HttpServletResponse resp, HttpStatusCode status, String msg)
            throws IOException {
        resp.setStatus(status.code());
        resp.getWriter().write(msg);
        return null;
    }

}
