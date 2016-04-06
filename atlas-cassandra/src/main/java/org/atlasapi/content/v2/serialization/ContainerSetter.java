package org.atlasapi.content.v2.serialization;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.atlasapi.content.Container;
import org.atlasapi.content.v2.model.Content;
import org.atlasapi.content.v2.model.udt.BroadcastRef;
import org.atlasapi.content.v2.model.udt.ItemRef;
import org.atlasapi.content.v2.model.udt.ItemSummary;
import org.atlasapi.content.v2.model.udt.LocationSummary;

import com.codepoetics.protonpack.maps.MapStream;
import com.google.common.collect.ImmutableList;

public class ContainerSetter {

    private final LocationSummarySerialization locationSummary = new LocationSummarySerialization();
    private final ItemRefSerialization itemRef = new ItemRefSerialization();
    private final ItemSummarySerialization itemSummary = new ItemSummarySerialization();
    private final BroadcastRefSerialization broadcastRef = new BroadcastRefSerialization();

    public void serialize(Content internal, org.atlasapi.content.Content content) {
        if (!Container.class.isInstance(content)) {
            return;
        }

        Container container = (Container) content;
        ImmutableList<org.atlasapi.content.ItemRef> itemRefs = container.getItemRefs();
        if (itemRefs != null) {
            internal.setItemRefs(itemRefs
                    .stream()
                    .map(itemRef::serialize)
                    .filter(Objects::nonNull)
                    .collect(Collectors.toSet()));
        }

        Map<org.atlasapi.content.ItemRef, Iterable<org.atlasapi.content.BroadcastRef>> upcomingContent =
                container.getUpcomingContent();
        if (upcomingContent != null) {
            internal.setUpcomingContent(MapStream.of(upcomingContent)
                    .mapEntries(
                            itemRef::serialize,
                            broadcastRefs -> StreamSupport.stream(broadcastRefs.spliterator(), false)
                                    .map(broadcastRef::serialize)
                                    .collect(Collectors.toList())
                    ).collect());
        }

        Map<org.atlasapi.content.ItemRef, Iterable<org.atlasapi.content.LocationSummary>> availableContent =
                container.getAvailableContent();
        if (availableContent != null) {
            internal.setAvailableContent(MapStream.of(availableContent)
                    .mapEntries(
                            itemRef::serialize,
                            locationSummaries -> StreamSupport.stream(
                                    locationSummaries.spliterator(),
                                    false
                            )
                                    .map(locationSummary::serialize)
                                    .collect(Collectors.toList())
                    ).collect());
        }

        List<org.atlasapi.content.ItemSummary> itemSummaries = container.getItemSummaries();
        if (itemSummaries != null) {
            internal.setItemSummaries(itemSummaries
                    .stream()
                    .map(itemSummary::serialize)
                    .collect(Collectors.toSet()));
        }
    }

    public void deserialize(org.atlasapi.content.Content content,
            org.atlasapi.content.v2.model.Content internal) {
        Container container = (Container) content;

        Set<ItemRef> itemRefs = internal.getItemRefs();
        if (itemRefs != null) {
            container.setItemRefs(itemRefs.stream()
                    .map(itemRef::deserialize)
                    .collect(Collectors.toList()));
        }

        Map<ItemRef, List<BroadcastRef>> internalUpcomingContent = internal.getUpcomingContent();
        if (internalUpcomingContent != null) {
            Map<org.atlasapi.content.ItemRef, Iterable<org.atlasapi.content.BroadcastRef>> upcomingContent =
                    MapStream.of(internalUpcomingContent)
                            .mapEntries(
                                    itemRef::deserialize,
                                    broadcastRefs -> (Iterable<org.atlasapi.content.BroadcastRef>) broadcastRefs
                                            .stream()
                                            .map(broadcastRef::deserialize)
                                            .collect(Collectors.toList())
                            ).collect();
            container.setUpcomingContent(upcomingContent);
        }

        Map<ItemRef, List<LocationSummary>> internalAvailableContent = internal.getAvailableContent();
        if (internalAvailableContent != null) {
            Map<org.atlasapi.content.ItemRef, Iterable<org.atlasapi.content.LocationSummary>> availableContent = MapStream.of(
                    internalAvailableContent
            ).mapEntries(
                    itemRef::deserialize,
                    locationSummaries -> (Iterable<org.atlasapi.content.LocationSummary>)
                            locationSummaries.stream()
                                    .map(locationSummary::deserialize)
                                    .collect(Collectors.toList())
            ).collect();
            container.setAvailableContent(availableContent);
        }

        Set<ItemSummary> itemSummaries = internal.getItemSummaries();
        if (itemSummaries != null) {
            container.setItemSummaries(itemSummaries
                    .stream()
                    .map(itemSummary::deserialize)
                    .collect(Collectors.toList()));
        }
    }
}