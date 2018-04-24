package org.atlasapi.system.bootstrap;

import com.fasterxml.jackson.databind.ObjectMapper;
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
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;

import java.io.IOException;
import java.util.Collection;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkNotNull;

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
    @RequestMapping(value = "/system/bootstrap/schedule", method = RequestMethod.POST, produces = "application/json")
    public ResponseEntity<?> bootstrapSchedule(
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
            return failure(HttpStatus.BAD_REQUEST, "Unknown source " + src);
        }

        Optional<Channel> channel = resolve(channelId);
        if (!channel.isPresent()) {
            return failure(HttpStatus.BAD_REQUEST, "Unknown channel " + channelId);
        }

        LocalDate date;
        try {
            date = dateParser.parseLocalDate(day);
        } catch (IllegalArgumentException iae) {
            return dateParseFailure(day);
        }

        try {
            ScheduleBootstrapper.Status status = scheduleBootstrapper.bootstrapSchedules(
                    ImmutableList.of(channel.get()),
                    interval(date),
                    source.get(),
                    migrateContent,
                    writeEquivs,
                    forwarding

            );
            return ResponseEntity.<ScheduleBootstrapResponse>ok().body(new ScheduleBootstrapResponse(status));
        } catch (Exception e) {
            return failure(HttpStatus.INTERNAL_SERVER_ERROR, Throwables.getStackTraceAsString(e));
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
    @RequestMapping(value = "/system/bootstrap/schedule/{channelId}", method = RequestMethod.POST, produces = "application/json")
    public ResponseEntity<?> bootstrapSchedule(
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
            return failure(HttpStatus.BAD_REQUEST, "Unknown source " + src);
        }

        Optional<Channel> channel = resolve(channelId);
        if (!channel.isPresent()) {
            return failure(HttpStatus.BAD_REQUEST, "Unknown channel " + channelId);
        }

        final LocalDate dateFrom;
        final LocalDate dateTo;
        try {
            dateFrom = dateParser.parseLocalDate(from);
        } catch (IllegalArgumentException iae) {
            return dateParseFailure(from);
        }

        try {
            dateTo = dateParser.parseLocalDate(to);
        } catch (IllegalArgumentException iae) {
            return failure(HttpStatus.BAD_REQUEST, "Failed to parse " + to + ", expected yyyy-MM-dd");
        }
        final Interval interval = interval(dateFrom, dateTo);
        try {
            ScheduleBootstrapper.Status status = scheduleBootstrapper.bootstrapSchedules(
                    ImmutableList.of(channel.get()),
                    interval,
                    source.get(),
                    migrateContent,
                    writeEquivs,
                    forwarding

            );
            return ResponseEntity.<ScheduleBootstrapResponse>ok().body(new ScheduleBootstrapResponse(status));
        } catch (Exception e) {
            return failure(HttpStatus.INTERNAL_SERVER_ERROR, Throwables.getStackTraceAsString(e));
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
    @RequestMapping(value = "/system/bootstrap/schedule/all", method = RequestMethod.POST, produces = "application/json")
    public ResponseEntity<?> bootstrapAllSchedules(
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
            return failure(HttpStatus.BAD_REQUEST, "Unknown source " + src);
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
            return dateParseFailure(from);
        }

        try {
            dateTo = dateParser.parseLocalDate(to);
        } catch (IllegalArgumentException iae) {
            return failure(HttpStatus.BAD_REQUEST, "Failed to parse " + to + ", expected yyyy-MM-dd");
        }
        final Interval interval = interval(dateFrom, dateTo);
        ScheduleBootstrapper.Status status = scheduleBootstrapper.bootstrapSchedules(
                channels,
                interval,
                source.get(),
                migrateContent,
                writeEquivs,
                forwarding
        );
        return ResponseEntity.<ScheduleBootstrapResponse>ok().body(new ScheduleBootstrapResponse(status));
    }

    @RequestMapping(value = "/system/bootstrap/schedule/all/status.json",
            method = RequestMethod.GET, produces = "application/json")
    public ResponseEntity<?> checkScheduleBootstrapStatus() throws IOException {
        Collection<ScheduleBootstrapper.Status> status = scheduleBootstrapper.getProgress();
        return ResponseEntity.<ScheduleBootstrapResponse>ok().body(new ScheduleBootstrapResponse(status));
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

    private ResponseEntity<?> dateParseFailure(String day)
            throws IOException {
        return failure(HttpStatus.BAD_REQUEST, "Failed to parse " + day + ", expected yyyy-MM-dd");
    }
    private ResponseEntity<?> failure(HttpStatus status, String msg)
            throws IOException {
        return ResponseEntity.<String>status(status).body(msg);
    }

    public class ScheduleBootstrapResponse {
        private Collection<ScheduleBootstrapper.Status> status;

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

    }

}
