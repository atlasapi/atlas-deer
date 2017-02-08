package org.atlasapi.content.v2.serialization.setters;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.atlasapi.content.Container;
import org.atlasapi.content.v2.model.Content;
import org.atlasapi.content.v2.model.udt.PartialItemRef;
import org.atlasapi.content.v2.model.udt.ItemRefAndBroadcastRefs;
import org.atlasapi.content.v2.model.udt.ItemRefAndItemSummary;
import org.atlasapi.content.v2.model.udt.ItemRefAndLocationSummaries;
import org.atlasapi.content.v2.model.udt.ItemSummary;
import org.atlasapi.content.v2.model.udt.Ref;
import org.atlasapi.content.v2.serialization.BroadcastRefSerialization;
import org.atlasapi.content.v2.serialization.ItemRefSerialization;
import org.atlasapi.content.v2.serialization.ItemSummarySerialization;
import org.atlasapi.content.v2.serialization.LocationSummarySerialization;
import org.atlasapi.content.v2.serialization.RefSerialization;

import com.metabroadcast.common.stream.MoreCollectors;

import com.google.common.collect.ImmutableList;

public class ContainerSetter {

    private final LocationSummarySerialization locationSummary = new LocationSummarySerialization();
    private final ItemRefSerialization itemRef = new ItemRefSerialization();
    private final RefSerialization ref = new RefSerialization();
    private final ItemSummarySerialization itemSummary = new ItemSummarySerialization();
    private final BroadcastRefSerialization broadcastRef = new BroadcastRefSerialization();

    public void serialize(Content internal, org.atlasapi.content.Content content) {
        if (!Container.class.isInstance(content)) {
            return;
        }

        Container container = (Container) content;
        ImmutableList<org.atlasapi.content.ItemRef> itemRefs = container.getItemRefs();
        if (itemRefs != null && !itemRefs.isEmpty()) {
            internal.setItemRefs(
                    itemRefs
                            .stream()
                            .collect(MoreCollectors.toImmutableMap(
                                    ref::serialize,
                                    itemRef::serialize
                            ))
            );
        }

        Map<org.atlasapi.content.ItemRef, Iterable<org.atlasapi.content.BroadcastRef>> upcomingContent =
                container.getUpcomingContent();
        if (upcomingContent != null && !upcomingContent.isEmpty()) {
            internal.setUpcomingContent(
                    upcomingContent.entrySet().stream().collect(MoreCollectors.toImmutableMap(
                            entry -> ref.serialize(entry.getKey()),
                            entry -> new ItemRefAndBroadcastRefs(
                                    itemRef.serialize(entry.getKey()),
                                    StreamSupport.stream(entry.getValue().spliterator(), false)
                                            .map(broadcastRef::serialize)
                                            .collect(MoreCollectors.toImmutableList())
                            )
                    ))
            );
        }

        Map<org.atlasapi.content.ItemRef, Iterable<org.atlasapi.content.LocationSummary>> availableContent =
                container.getAvailableContent();
        if (availableContent != null && !availableContent.isEmpty()) {
            internal.setAvailableContent(
                    availableContent.entrySet().stream().collect(MoreCollectors.toImmutableMap(
                            entry -> ref.serialize(entry.getKey()),
                            entry -> new ItemRefAndLocationSummaries(
                                    itemRef.serialize(entry.getKey()),
                                    StreamSupport.stream(entry.getValue().spliterator(), false)
                                            .map(locationSummary::serialize)
                                            .collect(MoreCollectors.toImmutableList())
                            )
                    ))
            );
        }

        List<org.atlasapi.content.ItemSummary> itemSummaries = container.getItemSummaries();
        if (itemSummaries != null && !itemSummaries.isEmpty()) {
            internal.setItemSummaries(
                    itemSummaries.stream().collect(MoreCollectors.toImmutableMap(
                            summary -> ref.serialize(summary.getItemRef()),
                            summary -> new ItemRefAndItemSummary(
                                    itemRef.serialize(summary.getItemRef()),
                                    itemSummary.serialize(summary)
                            )
                    ))
            );
        }
    }

    public void deserialize(org.atlasapi.content.Content content,
            org.atlasapi.content.v2.model.Content internal) {
        Container container = (Container) content;

        Map<Ref, PartialItemRef> itemRefs = internal.getItemRefs();
        if (itemRefs != null) {
            container.setItemRefs(
                    itemRefs.entrySet().stream()
                            .map(entry -> itemRef.deserialize(entry.getKey(), entry.getValue()))
                            .collect(Collectors.toList())
            );
        }

        Map<Ref, ItemRefAndBroadcastRefs> internalUpcomingContent = internal.getUpcomingContent();
        if (internalUpcomingContent != null) {
            Map<org.atlasapi.content.ItemRef, Iterable<org.atlasapi.content.BroadcastRef>> upcomingContent =
                    internalUpcomingContent.entrySet().stream()
                            .collect(MoreCollectors.toImmutableMap(
                                    entry -> {
                                        Ref ref = entry.getKey();
                                        ItemRefAndBroadcastRefs broadcasts = entry.getValue();

                                        return itemRef.deserialize(ref, broadcasts.getItemRef());
                                    },
                                    entry -> entry.getValue()
                                            .getBroadcastRefs()
                                            .stream()
                                            .map(broadcastRef::deserialize)
                                            .collect(MoreCollectors.toImmutableList())
                            ));

            container.setUpcomingContent(upcomingContent);
        }

        Map<Ref, ItemRefAndLocationSummaries> internalAvailableContent = internal.getAvailableContent();
        if (internalAvailableContent != null) {
            Map<org.atlasapi.content.ItemRef, Iterable<org.atlasapi.content.LocationSummary>> availableContent = internalAvailableContent
                    .entrySet()
                    .stream()
                    .collect(MoreCollectors.toImmutableMap(
                            entry -> {
                                Ref ref = entry.getKey();
                                ItemRefAndLocationSummaries locations = entry.getValue();

                                return itemRef.deserialize(ref, locations.getItemRef());
                            },
                            entry -> entry.getValue()
                                    .getLocationSummaries()
                                    .stream()
                                    .map(locationSummary::deserialize)
                                    .collect(MoreCollectors.toImmutableList())
                    ));

            container.setAvailableContent(availableContent);
        }

        Map<Ref, ItemRefAndItemSummary> itemSummaries = internal.getItemSummaries();
        if (itemSummaries != null) {
            container.setItemSummaries(
                    itemSummaries.entrySet().stream().map(entry -> {
                        Ref ref = entry.getKey();
                        ItemRefAndItemSummary summaryWithRef = entry.getValue();
                        ItemSummary summary = summaryWithRef.getSummary();

                        return itemSummary.deserialize(ref, summaryWithRef.getItemRef(), summary);
                    }).collect(MoreCollectors.toImmutableList())
            );
        }
    }
}