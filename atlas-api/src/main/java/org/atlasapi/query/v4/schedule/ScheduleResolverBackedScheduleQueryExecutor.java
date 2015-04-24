package org.atlasapi.query.v4.schedule;

import static com.google.common.base.Preconditions.checkNotNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

import java.util.List;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.FluentIterable;
import com.metabroadcast.common.base.Maybe;
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
import org.atlasapi.query.common.QueryContext;
import org.atlasapi.query.common.QueryExecutionException;
import org.atlasapi.query.common.QueryResult;
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

import javax.annotation.Nullable;


public class ScheduleResolverBackedScheduleQueryExecutor implements ScheduleQueryExecutor {

    private static final Function<ItemAndBroadcast, Id> IAB_TO_ID = Functions.compose(Identifiables.toId(), ItemAndBroadcast.toItem());

    private static final long QUERY_TIMEOUT = 60000;

    private ChannelResolver channelResolver;
    private ScheduleResolver scheduleResolver;
    private MergingEquivalentsResolver<Content> mergingContentResolver;

    public ScheduleResolverBackedScheduleQueryExecutor(ChannelResolver channelResolver,
            ScheduleResolver scheduleResolver, MergingEquivalentsResolver<Content> mergingContentResolver) {
        this.channelResolver = checkNotNull(channelResolver);
        this.scheduleResolver = checkNotNull(scheduleResolver);
        this.mergingContentResolver = checkNotNull(mergingContentResolver);
    }

    @Override
    public QueryResult<ChannelSchedule> execute(ScheduleQuery query)
            throws QueryExecutionException {
        
        Iterable<Channel> channels = resolveChannels(query);
        if(!query.getEnd().isPresent()) {
            throw new UnsupportedOperationException("'count' parameter not supported in non-equivalent schedule store. Please specify 'to' parameter in your request");
        }
        ListenableFuture<Schedule> schedule = scheduleResolver.resolve(
                channels,
                new Interval(query.getStart(), query.getEnd().get()),
                query.getSource()
        );
        
        if (query.isMultiChannel()) {
            return QueryResult.listResult(channelSchedules(schedule, query), query.getContext());
        }
        return QueryResult.singleResult(Iterables.getOnlyElement(channelSchedules(schedule, query)), query.getContext());
    }

    private Iterable<Channel> resolveChannels(ScheduleQuery query) throws QueryExecutionException {
        Iterable<Id> channelIds;

        if (query.isMultiChannel()) {
            channelIds = query.getChannelIds();
        } else {
            channelIds = ImmutableSet.of(query.getChannelId());
        }
        return Futures.get(Futures.transform(channelResolver.resolveIds(channelIds), new Function<Resolved<Channel>, Iterable<Channel>>() {
            @Override
            public Iterable<Channel> apply(Resolved<Channel> input) {
                return input.getResources();
            }
        }), 1, TimeUnit.MINUTES, QueryExecutionException.class);
    }

    private List<ChannelSchedule> channelSchedules(ListenableFuture<Schedule> schedule, ScheduleQuery query)
            throws ScheduleQueryExecutionException {
        
        if (query.getContext().getApplicationSources().isPrecedenceEnabled()) {
            schedule = Futures.transform(schedule, toEquivalentEntries(query));
        }
        
        return Futures.get(schedule,
                QUERY_TIMEOUT, MILLISECONDS, ScheduleQueryExecutionException.class).channelSchedules(); 
    }

    private AsyncFunction<Schedule, Schedule> toEquivalentEntries(final ScheduleQuery query) {
        return new AsyncFunction<Schedule, Schedule>() {
            @Override
            public ListenableFuture<Schedule> apply(Schedule input) {
                return resolveEquivalents(input, query.getContext());
            }
        };
    }

    private ListenableFuture<Schedule> resolveEquivalents(Schedule schedule,
            QueryContext context) {
        ApplicationSources sources = context.getApplicationSources();
        ImmutableSet<Annotation> annotations = context.getAnnotations().all();
        ListenableFuture<ResolvedEquivalents<Content>> equivs
            = mergingContentResolver.resolveIds(idsFrom(schedule), sources, annotations);
        return Futures.transform(equivs, intoSchedule(schedule)); 
    }

    private Iterable<Id> idsFrom(Schedule schedule) {
        List<ChannelSchedule> channelSchedules = schedule.channelSchedules();
        List<ImmutableList<ItemAndBroadcast>> entries = Lists.transform(channelSchedules, ChannelSchedule.toEntries());
        return ImmutableSet.copyOf(Iterables.transform(Iterables.concat(entries), IAB_TO_ID));
    }

    private Function<ResolvedEquivalents<Content>, Schedule> intoSchedule(final Schedule schedule) {
        return new Function<ResolvedEquivalents<Content>, Schedule>(){
            @Override
            public Schedule apply(ResolvedEquivalents<Content> input) {
                ImmutableList.Builder<ChannelSchedule> transformed = ImmutableList.builder(); 
                for (ChannelSchedule cs : schedule.channelSchedules()) {
                    transformed.add(cs.copyWithEntries(replaceItems(cs.getEntries(), input)));
                }
                return new Schedule(transformed.build(), schedule.interval());
            }
        };
    }

    private List<ItemAndBroadcast> replaceItems(List<ItemAndBroadcast> entries,
            final ResolvedEquivalents<Content> equivs) {
        return Lists.transform(entries, new Function<ItemAndBroadcast, ItemAndBroadcast>() {
            @Override
            public ItemAndBroadcast apply(ItemAndBroadcast input) {
                Item item = (Item) Iterables.getOnlyElement(equivs.get(input.getItem().getId()));
                replaceBroadcasts(item, input.getBroadcast());
                return new ItemAndBroadcast(item, input.getBroadcast());
            }

        });
    }

    private void replaceBroadcasts(Item item, Broadcast broadcast) {
        item.setBroadcasts(ImmutableSet.of(broadcast));
    }
}
