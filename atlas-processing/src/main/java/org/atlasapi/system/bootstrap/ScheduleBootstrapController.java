package org.atlasapi.system.bootstrap;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.datatype.joda.JodaModule;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.metabroadcast.common.ids.NumberToShortStringCodec;
import com.metabroadcast.common.ids.SubstitutionTableNumberCodec;
import com.metabroadcast.common.time.DateTimeZones;
import org.atlasapi.channel.Channel;
import org.atlasapi.channel.ChannelResolver;
import org.atlasapi.entity.Id;
import org.atlasapi.entity.util.Resolved;
import org.atlasapi.media.channel.ChannelQuery;
import org.atlasapi.media.entity.Publisher;
import org.elasticsearch.common.base.Throwables;
import org.joda.time.Interval;
import org.joda.time.LocalDate;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;

import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.Collection;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkNotNull;

@Controller
public class ScheduleBootstrapController {

    private final ChannelResolver channelResolver;
    private final ScheduleBootstrapper scheduleBootstrapper;
    private final ObjectMapper jsonMapper;

    private static final DateTimeFormatter dateParser = ISODateTimeFormat.date();
    private static final NumberToShortStringCodec idCodec = SubstitutionTableNumberCodec.lowerCaseOnly();
    private static final Logger log = LoggerFactory.getLogger(ScheduleBootstrapper.class);

    public ScheduleBootstrapController(
            ChannelResolver channelResvoler,
            ScheduleBootstrapper scheduleBootstrapper
    ) {
        this.scheduleBootstrapper = checkNotNull(scheduleBootstrapper);
        this.channelResolver = checkNotNull(channelResvoler);
        jsonMapper = new ObjectMapper();
        jsonMapper.registerModules(
                new JavaTimeModule(),
                new JodaModule(),
                new Jdk8Module()
        );
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
    public void bootstrapSchedule(
            HttpServletResponse response,
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

        Optional<Publisher> source = Publisher.fromKey(src).toGuavaOptional();
        if (!source.isPresent()) {
            failure(response, HttpServletResponse.SC_BAD_REQUEST, "Unknown source " + src);
            return;
        }

        Optional<Channel> channel = resolve(channelId);
        if (!channel.isPresent()) {
            failure(response, HttpServletResponse.SC_BAD_REQUEST, "Unknown channel " + channelId);
            return;
        }

        LocalDate date;
        try {
            date = dateParser.parseLocalDate(day);
        } catch (IllegalArgumentException iae) {
            dateParseFailure(response, day);
            return;
        }

        try {
            ScheduleBootstrapper.Status status = scheduleBootstrapper.bootstrapSchedules(
                    ImmutableList.of(channel.get()),
                    interval(date),
                    source.get(),
                    migrateContent,
                    writeEquivs,
                    forwarding,
                    false

            );
            response.setStatus(HttpServletResponse.SC_OK);
            response.setContentType(MediaType.APPLICATION_JSON_VALUE);
            jsonMapper.writeValue(response.getOutputStream(), new ScheduleBootstrapResponse(status));
            response.flushBuffer();
        } catch (Exception e) {
            failure(response, HttpServletResponse.SC_INTERNAL_SERVER_ERROR, Throwables.getStackTraceAsString(e));
        }
    }

    /**
     * Bootstrap a single channel for a a date range.
     * <p><em>Note:</em> {@code migrateContent}, {@code writeEquivs} and {@code forwarding}
     * are mutually exclusive, and have that respective precedence.</p>
     * @param channelId     the channel id (lowercase encoded string form)
     * @param src   the source/publisher
     * @param from  the date to migrate from
     * @param to    the date to migrate to
     * @param migrateContent    true to migrate content hierachy as well
     * @param writeEquivs       true to migrate content equivalences as well
     * @param forwarding        true to forward to the schedule equivalence writer as well
     */
    @RequestMapping(value = "/system/bootstrap/schedule/{channelId}", method = RequestMethod.POST)
    public void bootstrapSchedule(
            HttpServletResponse response,
            @PathVariable("channelId") String channelId,
            @RequestParam("source") String src,
            @RequestParam("from") String from,
            @RequestParam("to") String to,
            @RequestParam(value = "migrateContent", required = false,
                    defaultValue = "false") boolean migrateContent,
            @RequestParam(value = "writeEquivalences", required = false,
                    defaultValue = "false") boolean writeEquivs,
            @RequestParam(value = "forwarding", required = false,
                    defaultValue = "false") boolean forwarding
    ) throws Exception {

        Optional<Publisher> source = Publisher.fromKey(src).toGuavaOptional();
        if (!source.isPresent()) {
            failure(response, HttpServletResponse.SC_BAD_REQUEST, "Unknown source " + src);
            return;
        }

        Optional<Channel> channel = resolve(channelId);
        if (!channel.isPresent()) {
            failure(response, HttpServletResponse.SC_BAD_REQUEST, "Unknown channel " + channelId);
            return;
        }

        final LocalDate dateFrom;
        final LocalDate dateTo;
        try {
            dateFrom = dateParser.parseLocalDate(from);
        } catch (IllegalArgumentException iae) {
            dateParseFailure(response, from);
            return;
        }

        try {
            dateTo = dateParser.parseLocalDate(to);
        } catch (IllegalArgumentException iae) {
            dateParseFailure(response, to);
            return;
        }
        final Interval interval = interval(dateFrom, dateTo);
        try {
            ScheduleBootstrapper.Status status = scheduleBootstrapper.bootstrapSchedules(
                    ImmutableList.of(channel.get()),
                    interval,
                    source.get(),
                    migrateContent,
                    writeEquivs,
                    forwarding,
                    false
            );
            if(status.getFailures() > 0) {
                Set<Throwable> errors = status.getThrowables();
                if(errors.isEmpty()) {
                    failure(response, HttpServletResponse.SC_INTERNAL_SERVER_ERROR, "Failed to migrate channel");
                    return;
                }
                Throwable t = errors.iterator().next();
                failure(response, HttpServletResponse.SC_INTERNAL_SERVER_ERROR, String.format("%s: %s",
                        t.getMessage(), Throwables.getStackTraceAsString(t)));
                return;
            }
            response.setStatus(HttpServletResponse.SC_OK);
            response.setContentType(MediaType.APPLICATION_JSON_VALUE);
            jsonMapper.writeValue(response.getOutputStream(), new ScheduleBootstrapResponse(status));
            response.flushBuffer();
        } catch (Exception e) {
            failure(response, HttpServletResponse.SC_INTERNAL_SERVER_ERROR, Throwables.getStackTraceAsString(e));
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
            HttpServletResponse response,
            @RequestParam("source") String src,
            @RequestParam("from") String from,
            @RequestParam("to") String to,
            @RequestParam(value = "migrateContent", required = false,
                    defaultValue = "false") boolean migrateContent,
            @RequestParam(value = "writeEquivalences", required = false,
                    defaultValue = "false") boolean writeEquivs,
            @RequestParam(value = "forwarding", required = false,
                    defaultValue = "false") boolean forwarding
    ) throws Exception {        // NOSONAR

        final Optional<Publisher> source = Publisher.fromKey(src).toGuavaOptional();
        if (!source.isPresent()) {
            failure(response, HttpServletResponse.SC_BAD_REQUEST, "Unknown source " + src);
            return;
        }

        final Iterable<Channel> channels =
                Futures.getChecked(channelResolver.resolveChannels(ChannelQuery.builder().build()),
                        Exception.class,
                        1, TimeUnit.MINUTES
                ).getResources();

        final LocalDate dateFrom;
        final LocalDate dateTo;
        try {
            dateFrom = dateParser.parseLocalDate(from);
        } catch (IllegalArgumentException iae) {
            dateParseFailure(response, from);
            return;
        }

        try {
            dateTo = dateParser.parseLocalDate(to);
        } catch (IllegalArgumentException iae) {
            dateParseFailure(response, to);
            return;
        }
        final Interval interval = interval(dateFrom, dateTo);
        ScheduleBootstrapper.Status status = scheduleBootstrapper.bootstrapSchedules(
                channels,
                interval,
                source.get(),
                migrateContent,
                writeEquivs,
                forwarding,
                true
        );
        response.setStatus(HttpServletResponse.SC_OK);
        response.setContentType(MediaType.APPLICATION_JSON_VALUE);
        jsonMapper.writeValue(response.getOutputStream(), new ScheduleBootstrapResponse(status));
        response.flushBuffer();
    }

    @RequestMapping(value = "/system/bootstrap/schedule/all/status.json",
            method = RequestMethod.GET)
    public void checkScheduleBootstrapStatus(
            HttpServletResponse response
    ) throws IOException {
        Collection<ScheduleBootstrapper.Status> status = scheduleBootstrapper.getProgress();
        response.setStatus(HttpServletResponse.SC_OK);
        response.setContentType(MediaType.APPLICATION_JSON_VALUE);
        jsonMapper.writeValue(response.getOutputStream(), new ScheduleBootstrapResponse(status));
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

    private Optional<Channel> resolve(String channelId) throws Exception {  // NOSONAR
        Id cid = Id.valueOf(idCodec.decode(channelId));
        ListenableFuture<Resolved<Channel>> channelFuture = channelResolver.resolveIds(ImmutableList
                .of(cid));
        Resolved<Channel> resolvedChannel = Futures.getChecked(
                channelFuture,
                Exception.class,
                1, TimeUnit.MINUTES
        );

        if (resolvedChannel.getResources().isEmpty()) {
            return Optional.absent();
        }
        return Optional.of(Iterables.getOnlyElement(resolvedChannel.getResources()));
    }

    private void dateParseFailure(HttpServletResponse response, String day)
            throws IOException {
        failure(response, HttpServletResponse.SC_BAD_REQUEST, "Failed to parse " + day + ", expected yyyy-MM-dd");
    }
    private void failure(HttpServletResponse response, int statusCode, String msg)
            throws IOException {
        response.setStatus(statusCode);
        response.setContentType(MediaType.APPLICATION_JSON_VALUE);
        response.getWriter().write(msg);
    }

    public class ScheduleBootstrapResponse {
        private final Collection<ScheduleBootstrapper.Status> status;

        public ScheduleBootstrapResponse(ScheduleBootstrapper.Status status) {
            if(status == null) {
                this.status = ImmutableList.of();
            }
            else {
                this.status = ImmutableList.of(status);
            }
        }

        public ScheduleBootstrapResponse(Collection<ScheduleBootstrapper.Status> status) {
            this.status = ImmutableList.copyOf(checkNotNull(status));
        }

        public Collection<ScheduleBootstrapper.Status> getStatus() {
            return status;
        }

        public boolean isBootstrapping() { return !status.isEmpty(); }

    }

}
