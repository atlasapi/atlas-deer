package org.atlasapi.content.v2.serialization.setters;

import com.metabroadcast.common.stream.MoreCollectors;
import org.atlasapi.content.Broadcast;
import org.atlasapi.content.Item;
import org.atlasapi.content.v2.model.Content;
import org.atlasapi.content.v2.model.udt.SegmentEvent;
import org.atlasapi.content.v2.model.udt.UpdateTimes;
import org.atlasapi.content.v2.serialization.BroadcastSerialization;
import org.atlasapi.content.v2.serialization.ContainerRefSerialization;
import org.atlasapi.content.v2.serialization.ContainerSummarySerialization;
import org.atlasapi.content.v2.serialization.RestrictionSerialization;
import org.atlasapi.content.v2.serialization.SegmentEventSerialization;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import org.joda.time.Duration;

import static org.atlasapi.content.v2.serialization.DateTimeUtils.toInstant;

public class ItemSetter {

    private final BroadcastSerialization broadcast = new BroadcastSerialization();
    private final ContainerRefSerialization containerRef = new ContainerRefSerialization();
    private final SegmentEventSerialization segmentEvent = new SegmentEventSerialization();
    private final ContainerSummarySerialization containerSummary =
            new ContainerSummarySerialization();
    private final RestrictionSerialization restriction = new RestrictionSerialization();

    public void serialize(Content internal, org.atlasapi.content.Content content) {
        if (!Item.class.isInstance(content)) {
            return;
        }

        Item item = (Item) content;

        internal.setContainerRef(containerRef.serialize(item.getContainerRef()));
        internal.setIsLongForm(item.getIsLongForm());
        internal.setBlackAndWhite(item.getBlackAndWhite());
        internal.setSortKey(item.sortKey());

        Duration duration = item.getDuration();
        if(duration != null) {
            internal.setDuration(duration.getMillis());
        }

        internal.setContainerSummary(containerSummary.serialize(item.getContainerSummary()));

        internal.setBroadcasts(item.getBroadcasts().stream()
                .collect(MoreCollectors.toImmutableMap(
                        Broadcast::getSourceId,
                        broadcast::serialize
                )));

        internal.setSegmentEvents(item.getSegmentEvents()
                .stream()
                .map(segmentEvent::serialize)
                .filter(Objects::nonNull)
                // Order is not preserved when fetched from Owl since it is originally stored in a set
                // we need to preserve order in the resulting list so that content change checks will succeed
                // if nothing has changed
                .sorted(SegmentEvent.COMPARATOR)
                .collect(Collectors.toList()));

        internal.setRestrictions(item.getRestrictions().stream()
                .collect(SetterUtils.toImmutableMapAllowDuplicates(
                        restriction::serialize,
                        r -> new UpdateTimes(
                                toInstant(r.getLastUpdated()),
                                toInstant(r.getEquivalenceUpdate())
                        )
                )));
    }

    public void deserialize(org.atlasapi.content.Content content, Content internal) {
        Item item = (Item) content;

        item.setContainerRef(containerRef.deserialize(internal.getContainerRef()));

        Boolean isLongForm = internal.getIsLongForm();
        if (isLongForm != null) {
            item.setIsLongForm(isLongForm);
        }

        item.setBlackAndWhite(internal.getBlackAndWhite());

        item = item.withSortKey(internal.getSortKey());

        Long duration = internal.getDuration();
        if (duration != null) {
            item.setDuration(new Duration(duration));
        }

        item.setContainerSummary(containerSummary.deserialize(internal.getContainerSummary()));

        Map<String, org.atlasapi.content.v2.model.udt.Broadcast> broadcasts = internal.getBroadcasts();
        if (broadcasts != null) {
            item.setBroadcasts(
                    broadcasts.entrySet().stream()
                            .map(entry -> broadcast.deserialize(
                                    entry.getKey(),
                                    entry.getValue()
                            )).collect(Collectors.toSet())
            );
        }

        List<SegmentEvent> segmentEvents = internal.getSegmentEvents();
        if (segmentEvents != null) {
            item.setSegmentEvents(segmentEvents.stream()
                    .map(segmentEvent::deserialize)
                    .collect(Collectors.toList()));
        }

        Map<org.atlasapi.content.v2.model.udt.Restriction, UpdateTimes> restrictions = internal.getRestrictions();
        if (restrictions != null) {
            item.setRestrictions(
                    restrictions.entrySet().stream()
                            .map(entry -> restriction.deserialize(
                                    entry.getValue(),
                                    entry.getKey()
                            )).collect(MoreCollectors.toImmutableSet())
            );
        }
    }


}