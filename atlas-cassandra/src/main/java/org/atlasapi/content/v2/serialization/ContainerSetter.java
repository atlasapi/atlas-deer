package org.atlasapi.content.v2.serialization;

import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.atlasapi.content.Container;
import org.atlasapi.content.v2.model.Content;

import com.codepoetics.protonpack.maps.MapStream;

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
        internal.setItemRefs(container.getItemRefs()
                .stream()
                .map(itemRef::serialize)
                .filter(Objects::nonNull)
                .collect(Collectors.toList()));

        internal.setUpcomingContent(MapStream.of(container.getUpcomingContent())
                .mapEntries(
                        itemRef::serialize,
                        broadcastRefs -> StreamSupport.stream(broadcastRefs.spliterator(), false)
                                .map(broadcastRef::serialize)
                                .collect(Collectors.toList())
                ).collect());

        internal.setAvailableContent(MapStream.of(container.getAvailableContent())
                .mapEntries(
                        itemRef::serialize,
                        locationSummaries -> StreamSupport.stream(
                                locationSummaries.spliterator(),
                                false
                        )
                                .map(locationSummary::serialize)
                                .collect(Collectors.toList())
                ).collect());

        internal.setItemSummaries(container.getItemSummaries()
                .stream()
                .map(itemSummary::serialize)
                .collect(Collectors.toList()));
    }

    public void deserialize(org.atlasapi.content.Content content,
            org.atlasapi.content.v2.model.Content internal) {
        Container container = (Container) content;

        container.setItemRefs(internal.getItemRefs().stream()
                .map(itemRef::deserialize)
                .collect(Collectors.toList()));

        Map<org.atlasapi.content.ItemRef, Iterable<org.atlasapi.content.BroadcastRef>> upcomingContent =
                MapStream.of(internal.getUpcomingContent())
                        .mapEntries(
                                itemRef::deserialize,
                                broadcastRefs -> (Iterable<org.atlasapi.content.BroadcastRef>) broadcastRefs
                                        .stream()
                                        .map(broadcastRef::deserialize)
                                        .collect(Collectors.toList())
                        ).collect();

        container.setUpcomingContent(upcomingContent);

        Map<org.atlasapi.content.ItemRef, Iterable<org.atlasapi.content.LocationSummary>> availableContent = MapStream.of(
                internal.getAvailableContent()
        ).mapEntries(
                itemRef::deserialize,
                locationSummaries -> (Iterable<org.atlasapi.content.LocationSummary>)
                        locationSummaries.stream()
                                .map(locationSummary::deserialize)
                                .collect(Collectors.toList())
        ).collect();
        container.setAvailableContent(availableContent);

        container.setItemSummaries(internal.getItemSummaries()
                .stream()
                .map(itemSummary::deserialize)
                .collect(Collectors.toList()));
    }
}