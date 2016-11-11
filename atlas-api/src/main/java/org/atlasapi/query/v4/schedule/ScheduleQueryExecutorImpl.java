package org.atlasapi.query.v4.schedule;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.apache.avro.generic.GenericData;
import org.atlasapi.annotation.Annotation;
import org.atlasapi.application.ApplicationAccessRole;
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
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.output.NotFoundException;
import org.atlasapi.query.common.QueryContext;
import org.atlasapi.query.common.QueryExecutionException;
import org.atlasapi.query.common.QueryResult;
import org.atlasapi.schedule.ChannelSchedule;
import org.atlasapi.schedule.Schedule;
import org.atlasapi.schedule.ScheduleResolver;

import com.metabroadcast.common.stream.MoreCollectors;

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

        if (!query.getEnd().isPresent()) {
            throw new UnsupportedOperationException(
                    "'count' parameter not supported in non-equivalent schedule store. Please specify 'to' parameter in your request");
        }

        Iterable<Channel> channels = resolveChannels(query);

        List<Channel> defaultChannels = Lists.newArrayList(channels);
        List<Channel> ebsChannels = new ArrayList<>();

        if (query.getContext().getApplicationSources().getAccessRoles().contains(
                ApplicationAccessRole.PREFER_EBS_SCHEDULE)) {

            ebsChannels = defaultChannels.stream()
                    .filter(channel -> channel.getAvailableFrom().contains(Publisher.BT_SPORT_EBS))
                    .collect(Collectors.toList());

            defaultChannels.removeAll(ebsChannels);
        }

        List<ChannelSchedule> channelScheduleList = getChannelScheduleList(ebsChannels, defaultChannels, query);

        if (query.isMultiChannel()) {
            return QueryResult.listResult(
                    channelScheduleList,
                    query.getContext(),
                    channelScheduleList.size()
            );
        }

        return QueryResult.singleResult(
                Iterables.getOnlyElement(channelScheduleList),
                query.getContext()
        );
    }

    private Iterable<Channel> resolveChannels(ScheduleQuery query) throws QueryExecutionException {
        Iterable<Id> channelIds = query.isMultiChannel() ?
                query.getChannelIds() : ImmutableSet.of(query.getChannelId());

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

        if (query.getContext().getApplicationSources().isPrecedenceEnabled()) {
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
        ApplicationSources sources = context.getApplicationSources();
        ImmutableSet<Annotation> annotations = context.getAnnotations().all();
        ListenableFuture<ResolvedEquivalents<Content>> equivs
                = mergingContentResolver.resolveIds(idsFrom(schedule), sources, annotations);
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

    private List<ChannelSchedule> getChannelScheduleList(
            List<Channel> ebsChannels,
            List<Channel> defaultChannels,
            ScheduleQuery query
    ) throws ScheduleQueryExecutionException {

        List<ChannelSchedule> ebsChannelSchedule = new ArrayList<>();
        List<ChannelSchedule> defaultChannelSchedule = new ArrayList<>();

        if (!ebsChannels.isEmpty()) {

            ListenableFuture<Schedule> ebsSchedule = scheduleResolver.resolve(
                    ebsChannels,
                    new Interval(query.getStart(), query.getEnd().get()),
                    Publisher.BT_SPORT_EBS
            );

            ebsChannelSchedule = channelSchedules(ebsSchedule, query);
        }

        if (!defaultChannels.isEmpty()) {

            ListenableFuture<Schedule> baseSchedule = scheduleResolver.resolve(
                    defaultChannels,
                    new Interval(query.getStart(), query.getEnd().get()),
                    query.getSource()
            );

            defaultChannelSchedule = channelSchedules(baseSchedule, query);
        }

        return ImmutableList.<ChannelSchedule>builder()
                .addAll(ebsChannelSchedule)
                .addAll(defaultChannelSchedule)
                .build();
    }
}
