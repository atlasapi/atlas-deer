package org.atlasapi.content.v2.serialization;

import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import org.atlasapi.content.Item;
import org.atlasapi.content.v2.model.Content;
import org.atlasapi.content.v2.model.udt.Restriction;
import org.atlasapi.content.v2.model.udt.SegmentEvent;

import com.metabroadcast.common.intl.Countries;
import com.metabroadcast.common.intl.Country;

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
        internal.setCountriesOfOrigin(
                item.getCountriesOfOrigin().stream()
                        .map(Country::code)
                        .collect(Collectors.toSet())
        );
        internal.setSortKey(item.sortKey());

        internal.setContainerSummary(containerSummary.serialize(item.getContainerSummary()));

        internal.setBroadcasts(item.getBroadcasts().stream()
                .map(broadcast::serialize)
                .filter(Objects::nonNull)
                .collect(Collectors.toSet()));

        internal.setSegmentEvents(item.getSegmentEvents()
                .stream()
                .map(segmentEvent::serialize)
                .filter(Objects::nonNull)
                .collect(Collectors.toList()));

        internal.setRestrictions(item.getRestrictions()
                .stream()
                .map(restriction::serialize)
                .filter(Objects::nonNull)
                .collect(Collectors.toSet()));
    }

    public void deserialize(org.atlasapi.content.Content content, Content internal) {
        Item item = (Item) content;

        item.setContainerRef(containerRef.deserialize(internal.getContainerRef()));

        Boolean isLongForm = internal.getIsLongForm();
        if (isLongForm != null) {
            item.setIsLongForm(isLongForm);
        }

        item.setBlackAndWhite(internal.getBlackAndWhite());

        Set<String> countriesOfOrigin = internal.getCountriesOfOrigin();
        if (countriesOfOrigin != null) {
            item.setCountriesOfOrigin(countriesOfOrigin.stream()
                    .map(Countries::fromCode)
                    .collect(Collectors.toSet()));
        }

        item = item.withSortKey(internal.getSortKey());

        item.setContainerSummary(containerSummary.deserialize(internal.getContainerSummary()));

        Set<org.atlasapi.content.v2.model.udt.Broadcast> broadcasts = internal.getBroadcasts();
        if (broadcasts != null) {
            item.setBroadcasts(broadcasts.stream()
                    .map(broadcast::deserialize)
                    .collect(Collectors.toSet()));
        }

        List<SegmentEvent> segmentEvents = internal.getSegmentEvents();
        if (segmentEvents != null) {
            item.setSegmentEvents(segmentEvents.stream()
                    .map(segmentEvent::deserialize)
                    .collect(Collectors.toList()));
        }

        Set<Restriction> restrictions = internal.getRestrictions();
        if (restrictions != null) {
            item.setRestrictions(restrictions.stream()
                    .map(restriction::deserialize)
                    .collect(Collectors.toSet()));
        }
    }


}