package org.atlasapi.system.bootstrap;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.atlasapi.channel.Channel;
import org.atlasapi.content.Broadcast;
import org.atlasapi.content.Container;
import org.atlasapi.content.ContainerRef;
import org.atlasapi.content.Content;
import org.atlasapi.content.ContentResolver;
import org.atlasapi.content.ContentStore;
import org.atlasapi.content.ContentVisitor;
import org.atlasapi.content.Episode;
import org.atlasapi.content.Item;
import org.atlasapi.content.ItemAndBroadcast;
import org.atlasapi.content.Series;
import org.atlasapi.content.SeriesRef;
import org.atlasapi.entity.Id;
import org.atlasapi.entity.util.ResolveException;
import org.atlasapi.entity.util.Resolved;
import org.atlasapi.entity.util.StoreException;
import org.atlasapi.entity.util.WriteException;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.schedule.ChannelSchedule;
import org.atlasapi.schedule.EquivalentScheduleWriter;
import org.atlasapi.schedule.Schedule;
import org.atlasapi.schedule.ScheduleHierarchy;
import org.atlasapi.schedule.ScheduleResolver;
import org.atlasapi.schedule.ScheduleWriter;
import org.elasticsearch.common.base.Objects;
import org.joda.time.Interval;
import org.joda.time.LocalDate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.api.client.repackaged.com.google.common.base.Throwables;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.metabroadcast.common.collect.OptionalMap;
import com.metabroadcast.common.scheduling.UpdateProgress;

/**
 * <p>Copies a schedule from a {@link ScheduleResolver} to a {@link ScheduleWriter}
 * for a given {@link Channel}/{@link LocalDate}/{@link Publisher} combo.</p>
 * 
 * <p>Items in the schedule are resolved via a {@link ContentResolver} to ensure
 * all broadcasts are found.</p>
 */
public class ChannelIntervalScheduleBootstrapTask implements Callable<UpdateProgress> {

    private static final Logger log =
        LoggerFactory.getLogger(ChannelIntervalScheduleBootstrapTask.class);

    private final ScheduleResolver scheduleResolver;
    private final ScheduleWriter scheduleWriter;
    private final ContentStore contentStore;
    
    private final Channel channel;
    private final Interval interval;
    private final Publisher source;
    private final Optional<ContentVisitor<?>> scheduleContentVisitor;
    private final Optional<EquivalentScheduleWriter> equivalenceUpdater;

    public ChannelIntervalScheduleBootstrapTask(ScheduleResolver scheduleResolver,
            ScheduleWriter scheduleWriter, ContentStore contentStore,
            Publisher source, Channel channel, Interval interval,
            Optional<ContentVisitor<?>> scheduleContentVisitor,
            Optional<EquivalentScheduleWriter> equivalenceUpdater) {
        this.scheduleResolver = checkNotNull(scheduleResolver);
        this.scheduleWriter = checkNotNull(scheduleWriter);
        this.contentStore = checkNotNull(contentStore);
        this.channel = checkNotNull(channel);
        this.interval = checkNotNull(interval);
        this.source = checkNotNull(source);
        this.scheduleContentVisitor = checkNotNull(scheduleContentVisitor);
        this.equivalenceUpdater = checkNotNull(equivalenceUpdater);
    }

    @Override
    public UpdateProgress call() throws Exception {
        ListenableFuture<Schedule> resolved =
            scheduleResolver.resolve(ImmutableSet.of(channel), interval, source);
        Schedule schedule = Futures.get(resolved, 1, TimeUnit.MINUTES, ResolveException.class);
        /* it's reasonable for there not to be a channel for a given source/channel combination
         * but there should be precisely one if any. 
         */
        if (!schedule.channelSchedules().isEmpty()) {
            return writeItemsIn(Iterables.getOnlyElement(schedule.channelSchedules()));
        } else {
            return UpdateProgress.START;
        }
    }

    private UpdateProgress writeItemsIn(ChannelSchedule channelSchedule) throws StoreException {
        checkState(channelSchedule.getChannel().equals(channel),
                "got schedule for %s not %s", channelSchedule.getChannel(), channel);
        Map<Id, Optional<Content>> scheduleItems = resolveItems(channelSchedule);
        Map<Id, Optional<Content>> containers = resolveContainers(Optional.presentInstances(scheduleItems.values()));
        
        ImmutableList.Builder<ScheduleHierarchy> schedule = ImmutableList.builder();
        for (ItemAndBroadcast iab : channelSchedule.getEntries()) {
            Broadcast broadcast = iab.getBroadcast();
            Optional<Content> possItem = scheduleItems.get(iab.getItem().getId());
            if (!possItem.isPresent()) {
                log.warn("content not resolved for broadcast " + broadcast);
            }
            Container topLevelContainer = topLevelContainer(iab, containers);
            Series series = series(iab, containers);
            iab = new ItemAndBroadcast(contentOrNull(possItem, Item.class),broadcast);
            schedule.add(new ScheduleHierarchy(iab, topLevelContainer, series));
        }
        ImmutableList<ScheduleHierarchy> builtSchedule = schedule.build();
        UpdateProgress progress = tryWrite(builtSchedule);
        notifyListener(builtSchedule);
        writeEquivalences(builtSchedule);
        return progress;
    }

    private void writeEquivalences(ImmutableList<ScheduleHierarchy> builtSchedule) {
        if (!equivalenceUpdater.isPresent()) {
            return;
        }

        EquivalentScheduleWriter updater = equivalenceUpdater.get();
        try {
            updater.updateContent(builtSchedule.stream()
                    .map(ScheduleHierarchy::getItemAndBroadcast)
                    .map(ItemAndBroadcast::getItem)
                    .collect(Collectors.toList()));
        } catch (WriteException e) {
            log.warn("Failed to update equivs {} {} {}", source, channel, interval);
            throw Throwables.propagate(e);
        }
    }

    private void notifyListener(List<ScheduleHierarchy> schedule) {
        if (!scheduleContentVisitor.isPresent()) {
            return;
        }
        
        for (ScheduleHierarchy slot : schedule) {
            slot.getItemAndBroadcast().getItem().accept(scheduleContentVisitor.get());
            if (slot.getPossibleSeries().isPresent()) {
                slot.getPossibleSeries().get().accept(scheduleContentVisitor.get());
            }
            if (slot.getPrimaryContainer().isPresent()) {
                slot.getPrimaryContainer().get().accept(scheduleContentVisitor.get());
            }
        }
    }
    private Container topLevelContainer(ItemAndBroadcast iab, Map<Id, Optional<Content>> containers) {
        ContainerRef containerRef = iab.getItem().getContainerRef();
        if (containerRef != null) {
            Id id = containerRef.getId();
            return contentOrNull(containers.get(id), Container.class);
        }
        return null;
    }
    
    private Series series(ItemAndBroadcast iab, Map<Id, Optional<Content>> containers) {
        Item item = iab.getItem();
        if (item instanceof Episode) {
            SeriesRef ref = ((Episode)item).getSeriesRef();
            if (ref != null) {
                Id id = ref.getId();
                return contentOrNull(containers.get(id), Series.class);
            }
        }
        return null;    
    }

    private <T extends Content> T contentOrNull(Optional<Content> possContent, Class<T> cls) {
        if (possContent.isPresent()) {
            Content content = possContent.get();
            if (cls.isInstance(content)) {
                return cls.cast(content);
            }
        }
        return null;
    }

    private UpdateProgress tryWrite(ImmutableList<ScheduleHierarchy> schedule) {
        try {
            scheduleWriter.writeSchedule(schedule, channel, interval);
            return new UpdateProgress(schedule.size(), 0);
        } catch (WriteException we) {
            log.warn(String.format("failed to update %s %s %s", source, channel, interval), we);
            return new UpdateProgress(0, schedule.size());
        }
    }

    private OptionalMap<Id, Content> resolveContainers(Iterable<Content> items) throws StoreException {
        ImmutableSet<Id> ids = containerIds(items);
        ListenableFuture<Resolved<Content>> resolved = contentStore.resolveIds(ids);
        return Futures.get(resolved, 1, TimeUnit.MINUTES, ResolveException.class).toMap();
    }

    private ImmutableSet<Id> containerIds(Iterable<Content> items) {
        ImmutableSet.Builder<Id> containerIds = ImmutableSet.builder(); 
        for (Item item : Iterables.filter(items, Item.class)) {
            if (item.getContainerRef() != null) {
                containerIds.add(item.getContainerRef().getId());
            }
            if (item instanceof Episode && ((Episode)item).getSeriesRef() != null) {
                containerIds.add(((Episode)item).getSeriesRef().getId());
            }
        }
        return containerIds.build();
    }

    private OptionalMap<Id, Content> resolveItems(ChannelSchedule channelSchedule)
            throws ResolveException {
        List<ItemAndBroadcast> entries = channelSchedule.getEntries();
        Set<Id> entryIds = Sets.newHashSetWithExpectedSize(entries.size());
        for (ItemAndBroadcast itemAndBroadcast : entries) {
            entryIds.add(itemAndBroadcast.getItem().getId());
        }
        ListenableFuture<Resolved<Content>> resolved = contentStore.resolveIds(entryIds);
        return Futures.get(resolved, 1, TimeUnit.MINUTES, ResolveException.class).toMap();
    }
    
    @Override
    public String toString() {
        return Objects.toStringHelper(getClass())
            .add("src", source)
            .add("channel", channel)
            .add("day", interval)
            .toString();
    }
}
