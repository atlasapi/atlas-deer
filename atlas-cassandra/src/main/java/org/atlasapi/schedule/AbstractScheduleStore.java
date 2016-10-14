package org.atlasapi.schedule;

import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.atlasapi.channel.Channel;
import org.atlasapi.content.Broadcast;
import org.atlasapi.content.BroadcastRef;
import org.atlasapi.content.Content;
import org.atlasapi.content.ContentStore;
import org.atlasapi.content.Episode;
import org.atlasapi.content.Item;
import org.atlasapi.content.ItemAndBroadcast;
import org.atlasapi.content.SeriesRef;
import org.atlasapi.entity.Id;
import org.atlasapi.entity.util.Resolved;
import org.atlasapi.entity.util.WriteException;
import org.atlasapi.entity.util.WriteResult;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.schedule.ScheduleRef.Builder;

import com.metabroadcast.common.queue.MessageSender;
import com.metabroadcast.common.queue.MessagingException;
import com.metabroadcast.common.time.DateTimeZones;
import com.metabroadcast.common.time.Timestamp;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.primitives.Longs;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * {@code AbstractScheduleStore} is a base implementation of a {@link ScheduleStore}. It first
 * checks the integrity of the provided {@link ScheduleHierarchy}s, persists all {@link Content} to
 * the {@link ContentStore} and then resolves and updates schedule as necessary.
 * <p>
 * Schedules are divided into discrete, contiguous blocks of regular duration, represented by {@link
 * ChannelSchedule}s. The block duration is determined by the concrete implementation. Blocks may be
 * empty, partially populated or fully populated. Blocks may also have entries in common if they
 * overlap the start or end of the block interval.
 * <p>
 * This base will also detect overwritten {@link Broadcast Broadcast}s and update them in the {@code
 * ContentStore}.
 */
public abstract class AbstractScheduleStore implements ScheduleStore {

    private static final Logger log = LoggerFactory.getLogger(AbstractScheduleStore.class);
    private final ContentStore contentStore;
    private final MessageSender<ScheduleUpdateMessage> messageSender;
    private final BroadcastContiguityCheck contiguityCheck;
    private final ScheduleBlockUpdater blockUpdater;

    public AbstractScheduleStore(ContentStore contentStore,
            MessageSender<ScheduleUpdateMessage> sender) {
        this.contentStore = checkNotNull(contentStore);
        this.messageSender = checkNotNull(sender);
        this.contiguityCheck = new BroadcastContiguityCheck();
        this.blockUpdater = new ScheduleBlockUpdater();
    }

    @Override
    public List<WriteResult<? extends Content, Content>> writeSchedule(
            List<ScheduleHierarchy> content, Channel channel,
            Interval interval) throws WriteException {
        if (content.isEmpty()) {
            return ImmutableList.of();
        }
        List<ItemAndBroadcast> itemsAndBroadcasts = itemsAndBroadcasts(content);
        checkArgument(
                broadcastHaveIds(itemsAndBroadcasts),
                "all broadcasts must have IDs"
        );
        checkArgument(broadcastsContiguous(itemsAndBroadcasts),
                "broadcasts of items on %s not contiguous in %s", channel, interval
        );
        Publisher source = getSource(content);

        List<WriteResult<? extends Content, Content>> writeResults = writeContent(content);
        if (!contentChanged(writeResults)) {
            return writeResults;
        }
        List<ChannelSchedule> currentBlocks = resolveCurrentScheduleBlocks(
                source,
                channel,
                interval
        );
        List<ChannelSchedule> staleBlocks = resolveStaleScheduleBlocks(source, channel, interval);
        ScheduleBlocksUpdate update = blockUpdater.updateBlocks(
                currentBlocks,
                staleBlocks,
                itemsAndBroadcasts,
                channel,
                interval
        );
        log.info(
                "Processing schedule update for {} {} {}: currentEntries:{}, update:{}, stale broadcasts:{}",
                source,
                channel.getId().longValue(),
                interval,
                updateLog(currentBlocks),
                updateLog(itemsAndBroadcasts),
                updateLog(update.getStaleEntries())
        );
        for (ItemAndBroadcast staleEntry : Iterables.concat(
                update.getStaleEntries(),
                update.getStaleContent()
        )) {
            updateStaleItemInContentStore(staleEntry);
        }
        doWrite(source, removeAdditionalBroadcasts(update.getUpdatedBlocks()));
        sendUpdateMessage(source, content, update, channel, interval);
        return writeResults;
    }

    private String updateLog(Iterable<ItemAndBroadcast> itemsAndBroadcasts) {
        StringBuilder update = new StringBuilder();
        for (ItemAndBroadcast itemsAndBroadcast : itemsAndBroadcasts) {
            update.append(
                    String.format(
                            " %s -> (%s -> %s)",
                            itemsAndBroadcast.getBroadcast().getSourceId(),
                            itemsAndBroadcast.getBroadcast().getTransmissionTime(),
                            itemsAndBroadcast.getBroadcast().getTransmissionEndTime()
                    )
            );
        }

        return update.toString();
    }

    private String updateLog(List<ChannelSchedule> blocks) {
        StringBuilder update = new StringBuilder();
        for (ChannelSchedule block : blocks) {
            update.append(updateLog(block.getEntries()));
        }
        return update.toString();
    }

    private void sendUpdateMessage(Publisher source, List<ScheduleHierarchy> content,
            ScheduleBlocksUpdate update, Channel channel, Interval interval) throws WriteException {
        try {
            String messageId = UUID.randomUUID().toString();
            Timestamp messageTimestamp = Timestamp.of(DateTime.now(DateTimeZones.UTC));
            ScheduleRef updateRef = scheduleRef(content, channel, interval);
            ImmutableSet<BroadcastRef> staleRefs = broadcastRefs(update.getStaleEntries());

            ScheduleUpdateMessage message = new ScheduleUpdateMessage(messageId, messageTimestamp,
                    new ScheduleUpdate(source, updateRef, staleRefs)
            );
            Id channelId = message.getScheduleUpdate().getSchedule().getChannel();
            messageSender.sendMessage(message, Longs.toByteArray(channelId.longValue()));
        } catch (MessagingException e) {
            throw new WriteException(e);
        }
    }

    private ImmutableSet<BroadcastRef> broadcastRefs(Set<ItemAndBroadcast> staleEntries) {
        ImmutableSet.Builder<BroadcastRef> refs = ImmutableSet.builder();
        for (ItemAndBroadcast staleEntry : staleEntries) {
            refs.add(staleEntry.getBroadcast().toRef());
        }
        return refs.build();
    }

    private ScheduleRef scheduleRef(List<ScheduleHierarchy> content, Channel channel,
            Interval interval) {
        Id cid = channel.getId();
        Builder builder = ScheduleRef.forChannel(cid, interval);
        for (ScheduleHierarchy scheduleHierarchy : content) {
            ItemAndBroadcast iab = scheduleHierarchy.getItemAndBroadcast();
            builder.addEntry(iab.getItem().getId(), iab.getBroadcast().toRef());
        }
        return builder.build();
    }

    private List<ChannelSchedule> removeAdditionalBroadcasts(List<ChannelSchedule> updatedBlocks) {
        ImmutableList.Builder<ChannelSchedule> blocks = ImmutableList.builder();
        for (ChannelSchedule block : updatedBlocks) {
            blocks.add(block.copyWithEntries(removeAdditionalBroadcasts(block.getEntries())));
        }
        return blocks.build();
    }

    private Iterable<ItemAndBroadcast> removeAdditionalBroadcasts(
            Iterable<ItemAndBroadcast> entries) {
        return Iterables.transform(entries, new Function<ItemAndBroadcast, ItemAndBroadcast>() {

            @Override
            public ItemAndBroadcast apply(ItemAndBroadcast input) {
                Item item = removeAllBroadcastsBut(input.getItem(), input.getBroadcast());
                return new ItemAndBroadcast(item, input.getBroadcast());
            }

            private Item removeAllBroadcastsBut(Item item, Broadcast broadcast) {
                Item copy = item.copy();
                if (copy.getBroadcasts().contains(broadcast)) {
                    copy.setBroadcasts(ImmutableSet.of(broadcast));
                } else {
                    copy.setBroadcasts(ImmutableSet.<Broadcast>of());
                }
                return copy;
            }
        });
    }

    /**
     * Resolve the current block(s) of schedule for a given source, channel and interval. All the
     * blocks overlapped, fully or partially, by the interval must be returned with all entries
     * fully populated.
     * <p>
     * If there is no data for block then an empty block must be returned.
     *
     * @param source
     * @param channel
     * @param interval
     * @return
     * @throws WriteException
     */
    protected abstract List<ChannelSchedule> resolveCurrentScheduleBlocks(Publisher source,
            Channel channel,
            Interval interval) throws WriteException;

    /**
     * Resolve past schedule blocks. These are blocks which are currently not in use and are
     * necessary to ensure that they are deleted in equivalent store.
     * <p>
     * If the store doesn't store past blocks it should return an empty list;
     *
     * @param source
     * @param channel
     * @param interval
     * @return
     */
    protected abstract List<ChannelSchedule> resolveStaleScheduleBlocks(
            Publisher source,
            Channel channel,
            Interval interval
    ) throws WriteException;

    /**
     * Write the blocks of schedule for a source.
     * <p>
     * The entries in the blocks may not necessarily fully populate a block. Entries in a block may
     * overlap the beginning or end of a block. The same entry will be included two or more adjacent
     * blocks if it overlaps their respective ends/beginnings.
     * <p>
     * For example broadcasts A, B, C may be written in blocks as
     * <pre>
     *  |A, B| | B | | B, C|
     *  </pre>
     * if the broadcast B is long enough to cover more than one entire block.
     *
     * @param source
     * @param blocks
     * @throws WriteException
     */
    protected abstract void doWrite(Publisher source, List<ChannelSchedule> blocks)
            throws WriteException;

    private void updateStaleItemInContentStore(ItemAndBroadcast entry) throws WriteException {
        Id id = entry.getItem().getId();
        ListenableFuture<Resolved<Content>> resolve = contentStore.resolveIds(ImmutableList.of(id));
        Resolved<Content> resolved2 = Futures.get(resolve,
                10, TimeUnit.SECONDS, WriteException.class
        );
        Item resolved = (Item) Iterables.getOnlyElement(resolved2.getResources());

        Broadcast broadcast = entry.getBroadcast();
        broadcast.setIsActivelyPublished(false);
        SeriesRef seriesRef = null;
        if (resolve instanceof Episode && ((Episode) resolved).getSeriesRef() != null) {
            seriesRef = ((Episode) resolved).getSeriesRef();
        }

        contentStore.writeBroadcast(
                resolved.toRef(),
                Optional.fromNullable(resolved.getContainerRef()),
                Optional.fromNullable(seriesRef),
                broadcast
        );
    }

    private Item updateBroadcast(String broadcastId, Item resolved) {
        for (Broadcast broadcast : resolved.getBroadcasts()) {
            if (broadcastId.equals(broadcast.getSourceId())) {
                //This will be more fun with an immutable model.
                broadcast.setIsActivelyPublished(false);
                return resolved;
            }
        }
        return resolved;
    }

    private <T extends Content> boolean contentChanged(
            List<WriteResult<? extends Content, Content>> writeResults) {
        return Iterables.any(writeResults, WriteResult.<Content, Content>writtenFilter());
    }

    private List<WriteResult<? extends Content, Content>> writeContent(
            List<ScheduleHierarchy> contents) throws WriteException {
        return WritableScheduleHierarchy.from(contents).writeTo(contentStore);
    }

    private Publisher getSource(List<ScheduleHierarchy> content) {
        Publisher source = null;
        Iterator<ScheduleHierarchy> contentIter = content.iterator();
        if (contentIter.hasNext()) {
            source = source(contentIter.next());
            while (contentIter.hasNext()) {
                checkSourcesAreEqual(source, source(contentIter.next()));
            }
        }
        return source;
    }

    private Publisher source(ScheduleHierarchy hier) {
        Publisher source = hier.getItemAndBroadcast().getItem().getSource();
        if (hier.getPrimaryContainer().isPresent()) {
            checkSourcesAreEqual(source, hier.getPrimaryContainer().get().getSource());
        }
        if (hier.getPossibleSeries().isPresent()) {
            checkSourcesAreEqual(source, hier.getPossibleSeries().get().getSource());
        }
        return source;
    }

    private void checkSourcesAreEqual(Publisher source, Publisher other) {
        checkArgument(source.equals(other), "Content must be from a single source");
    }

    private boolean broadcastsContiguous(List<ItemAndBroadcast> items) {
        return true;//contiguityCheck.apply(Lists.transform(items, ItemAndBroadcast.toBroadcast()));
    }

    private boolean broadcastHaveIds(List<ItemAndBroadcast> itemsAndBroadcasts) {
        for (ItemAndBroadcast itemAndBroadcast : itemsAndBroadcasts) {
            if (itemAndBroadcast.getBroadcast().getSourceId() == null) {
                return false;
            }
        }
        return true;
    }

    private List<ItemAndBroadcast> itemsAndBroadcasts(List<ScheduleHierarchy> hierarchies) {
        List<ItemAndBroadcast> relevantBroadcasts = Lists.newArrayListWithCapacity(hierarchies.size());
        for (ScheduleHierarchy hierarchy : hierarchies) {
            relevantBroadcasts.add(hierarchy.getItemAndBroadcast());
        }
        return relevantBroadcasts;
    }

}
