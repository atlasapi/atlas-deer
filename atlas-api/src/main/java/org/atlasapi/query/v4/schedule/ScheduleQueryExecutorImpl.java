package org.atlasapi.query.v4.schedule;

import java.util.List;
import java.util.concurrent.TimeUnit;

import com.metabroadcast.applications.client.model.internal.Application;
import org.atlasapi.annotation.Annotation;
import org.atlasapi.application.ApplicationSources;
import org.atlasapi.channel.Channel;
import org.atlasapi.channel.ChannelResolver;
import org.atlasapi.content.Broadcast;
import org.atlasapi.content.Content;
import org.atlasapi.content.Item;
import org.atlasapi.content.ItemAndBroadcast;
import org.atlasapi.entity.Id;
import org.atlasapi.entity.Identifiables;
import org.atlasapi.entity.util.Resolved;
import org.atlasapi.equivalence.MergingEquivalentsResolver;
import org.atlasapi.equivalence.ResolvedEquivalents;
import org.atlasapi.output.NotFoundException;
import org.atlasapi.query.common.QueryResult;
import org.atlasapi.query.common.context.QueryContext;
import org.atlasapi.query.common.exceptions.QueryExecutionException;
import org.atlasapi.schedule.ChannelSchedule;
import org.atlasapi.schedule.Schedule;
import org.atlasapi.schedule.ScheduleResolver;

import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.joda.time.Interval;

import static com.google.common.base.Preconditions.checkNotNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class ScheduleQueryExecutorImpl implements ScheduleQueryExecutor {

    private static final Function<ItemAndBroadcast, Id> IAB_TO_ID = Functions.compose(
            Identifiables.toId(),
            ItemAndBroadcast.toItem()
    );

    private static final long QUERY_TIMEOUT = 60000;

    private ChannelResolver channelResolver;
    private ScheduleResolver scheduleResolver;
    private MergingEquivalentsResolver<Content> mergingContentResolver;

    public ScheduleQueryExecutorImpl(ChannelResolver channelResolver,
            ScheduleResolver scheduleResolver,
            MergingEquivalentsResolver<Content> mergingContentResolver) {
        this.channelResolver = checkNotNull(channelResolver);
        this.scheduleResolver = checkNotNull(scheduleResolver);
        this.mergingContentResolver = checkNotNull(mergingContentResolver);
    }

    @Override
    public QueryResult<ChannelSchedule> execute(ScheduleQuery query)
            throws QueryExecutionException {

        Iterable<Channel> channels = resolveChannels(query);
        if (!query.getEnd().isPresent()) {
            throw new UnsupportedOperationException(
                    "'count' parameter not supported in non-equivalent schedule store. "
                            + "Please specify 'to' parameter in your request");
        }
        ListenableFuture<Schedule> schedule = scheduleResolver.resolve(
                channels,
                new Interval(query.getStart(), query.getEnd().get()),
                query.getSource()
        );

        if (query.isMultiChannel()) {
            List<ChannelSchedule> channelSchedules = channelSchedules(schedule, query);
            return QueryResult.listResult(
                    channelSchedules,
                    query.getContext(),
                    channelSchedules.size()
            );
        }
        return QueryResult.singleResult(
                Iterables.getOnlyElement(channelSchedules(schedule, query)),
                query.getContext()
        );
    }

    private Iterable<Channel> resolveChannels(ScheduleQuery query) throws QueryExecutionException {
        Iterable<Id> channelIds;
        if (query.isMultiChannel()) {
            channelIds = query.getChannelIds();
        } else {
            channelIds = ImmutableSet.of(query.getChannelId());
        }
        Resolved<Channel> resolvedChannels = Futures.get(
                channelResolver.resolveIds(channelIds),
                1,
                TimeUnit.MINUTES,
                QueryExecutionException.class
        );
        if (resolvedChannels.getResources().isEmpty()) {
            throw new NotFoundException(Iterables.getFirst(channelIds, null));
        }
        return resolvedChannels.getResources();
    }

    private List<ChannelSchedule> channelSchedules(ListenableFuture<Schedule> schedule,
            ScheduleQuery query)
            throws ScheduleQueryExecutionException {

        if (query.getContext().getApplication().getConfiguration().isPrecedenceEnabled()) {
            schedule = Futures.transform(schedule, toEquivalentEntries(query));
        }

        return Futures.get(schedule,
                QUERY_TIMEOUT, MILLISECONDS, ScheduleQueryExecutionException.class
        ).channelSchedules();
    }

    private AsyncFunction<Schedule, Schedule> toEquivalentEntries(final ScheduleQuery query) {
        return input -> resolveEquivalents(input, query.getContext());
    }

    private ListenableFuture<Schedule> resolveEquivalents(Schedule schedule,
            QueryContext context) {
        Application application = context.getApplication();
        ImmutableSet<Annotation> annotations = context.getAnnotations().all();
        ListenableFuture<ResolvedEquivalents<Content>> equivs
                = mergingContentResolver.resolveIds(idsFrom(schedule), application, annotations);
        return Futures.transform(equivs, intoSchedule(schedule));
    }

    private Iterable<Id> idsFrom(Schedule schedule) {
        List<ChannelSchedule> channelSchedules = schedule.channelSchedules();
        List<ImmutableList<ItemAndBroadcast>> entries = Lists.transform(
                channelSchedules,
                ChannelSchedule.toEntries()
        );
        return ImmutableSet.copyOf(Iterables.transform(Iterables.concat(entries), IAB_TO_ID));
    }

    private Function<ResolvedEquivalents<Content>, Schedule> intoSchedule(final Schedule schedule) {
        return input -> {
            ImmutableList.Builder<ChannelSchedule> transformed = ImmutableList.builder();
            for (ChannelSchedule cs : schedule.channelSchedules()) {
                transformed.add(cs.copyWithEntries(replaceItems(cs.getEntries(), input)));
            }
            return new Schedule(transformed.build(), schedule.interval());
        };
    }

    private List<ItemAndBroadcast> replaceItems(List<ItemAndBroadcast> entries,
            final ResolvedEquivalents<Content> equivs) {
        return Lists.transform(entries, input -> {
            Item item = (Item) Iterables.getOnlyElement(equivs.get(input.getItem().getId()));
            replaceBroadcasts(item, input.getBroadcast());
            return new ItemAndBroadcast(item, input.getBroadcast());
        });
    }

    private void replaceBroadcasts(Item item, Broadcast broadcast) {
        item.setBroadcasts(ImmutableSet.of(broadcast));
    }
}
