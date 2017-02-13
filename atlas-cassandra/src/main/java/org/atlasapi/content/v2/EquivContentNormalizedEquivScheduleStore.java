package org.atlasapi.content.v2;

import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.atlasapi.annotation.Annotation;
import org.atlasapi.channel.Channel;
import org.atlasapi.content.ContentResolver;
import org.atlasapi.content.Described;
import org.atlasapi.content.EquivalentContentStore;
import org.atlasapi.entity.Id;
import org.atlasapi.equivalence.EquivalenceGraphStore;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.schedule.CassandraEquivalentScheduleStore;
import org.atlasapi.schedule.EquivalentSchedule;

import com.metabroadcast.common.stream.MoreCollectors;
import com.metabroadcast.common.time.Clock;
import com.metabroadcast.promise.Promise;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.Session;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.ListenableFuture;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.joda.time.Interval;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkNotNull;

public class EquivContentNormalizedEquivScheduleStore extends CassandraEquivalentScheduleStore {

    private static final Logger log = LoggerFactory.getLogger(EquivContentNormalizedEquivScheduleStore.class);

    private static final Duration MAX_SCHEDULE_LENGTH = Duration.standardHours(24);

    private final Timer resolveScheduleTimer;
    private final Counter resolveScheduleCounter;
    private final Counter resolveContentErrorCounter;
    private final Histogram resolveContentIdNumHistogram;
    private final EquivalentContentStore equivContentStore;

    public EquivContentNormalizedEquivScheduleStore(
            EquivalenceGraphStore graphStore,
            EquivalentContentStore equivContentStore,
            ContentResolver contentStore,
            Session session,
            ConsistencyLevel read,
            ConsistencyLevel write,
            Clock clock,
            MetricRegistry metrics
    ) {
        super(graphStore, contentStore, session, read, write, clock, metrics,
                EquivContentNormalizedEquivScheduleStore.class.getSimpleName()
        );

        this.equivContentStore = checkNotNull(equivContentStore);

        this.resolveScheduleTimer = metrics.timer(String.format(
                "%s.resolveSchedule",
                getClass().getSimpleName()
        ));
        this.resolveScheduleCounter = metrics.counter(String.format(
                "%s.resolveScheduleCount",
                getClass().getSimpleName()
        ));
        this.resolveContentErrorCounter = metrics.counter(String.format(
                "%s.resolveContentErrorsCount",
                getClass().getSimpleName()
        ));
        this.resolveContentIdNumHistogram = metrics.histogram(String.format(
                "%s.resolveContentNumIds",
                getClass().getSimpleName()
        ));
    }

    @Override
    public ListenableFuture<EquivalentSchedule> resolveSchedules(
            Iterable<Channel> channels,
            Interval interval,
            Publisher source,
            Set<Publisher> selectedSources
    ) {
        Timer.Context timer = resolveScheduleTimer.time();
        resolveScheduleCounter.inc();

        return Promise.wrap(super.resolveSchedules(
                channels,
                interval,
                source,
                selectedSources
        )).then(sched -> {
            Set<Id> graphIds = sched.channelSchedules()
                    .stream()
                    .flatMap(chanSchedule -> chanSchedule.getEntries().stream())
                    .map(entry -> entry.getItems().getGraph().getId())
                    .collect(MoreCollectors.toImmutableSet());

            ImmutableSet<Publisher> publishers = sched.channelSchedules()
                    .stream()
                    .flatMap(chanSchedule -> chanSchedule.getEntries().stream())
                    .flatMap(entry -> entry.getItems().getResources().stream())
                    .map(Described::getSource)
                    .collect(MoreCollectors.toImmutableSet());

            resolveContentIdNumHistogram.update(graphIds.size());

            try {
                equivContentStore.resolveIds(
                        graphIds,
                        publishers,
                        Annotation.all()
                ).get(15L, TimeUnit.SECONDS);
            } catch (InterruptedException | ExecutionException | TimeoutException e) {
                resolveContentErrorCounter.inc();
                log.error("Failed to resolve a shitload of content", e);
            }

            timer.stop();

            return sched;
        });
    }

    @Override
    public ListenableFuture<EquivalentSchedule> resolveSchedules(
            Iterable<Channel> channels,
            DateTime start,
            Integer count,
            Publisher source,
            Set<Publisher> selectedSources
    ) {
        Interval interval = new Interval(start, start.plus(MAX_SCHEDULE_LENGTH));

        return Promise.wrap(resolveSchedules(channels, interval, source, selectedSources))
                .then(input -> input.withLimitedBroadcasts(count));
    }
}
