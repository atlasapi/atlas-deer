package org.atlasapi.content;

import java.util.Map;

import org.atlasapi.serialization.protobuf.CommonProtos;
import org.atlasapi.serialization.protobuf.ContentProtos;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

public class ItemAndLocationSummarySerializer {

    private final LocationSummarySerializer locationSummarySerializer = new LocationSummarySerializer();

    public ContentProtos.ItemAndLocationSummary.Builder serialize(ItemRef itemRef,
            Iterable<LocationSummary> locationSummaries) {

        ContentRefSerializer contentRefSerializer = new ContentRefSerializer(itemRef.getSource());
        CommonProtos.Reference.Builder itemRefBuilder = contentRefSerializer.serialize(itemRef);

        ContentProtos.ItemAndLocationSummary.Builder builder = ContentProtos.ItemAndLocationSummary.newBuilder();

        for (LocationSummary locationSummary : locationSummaries) {

            CommonProtos.LocationSummary.Builder locationSummaryBuilder = locationSummarySerializer.serialize(
                    locationSummary);

            builder.addLocationSummary(locationSummaryBuilder);
        }

        builder.setItem(itemRefBuilder);
        return builder;
    }

    public Map<ItemRef, Iterable<LocationSummary>> deserialize(
            Iterable<ContentProtos.ItemAndLocationSummary> itemAndLocationSummaries) {
        ContentRefSerializer contentRefSerializer = new ContentRefSerializer(null);
        Map<ItemRef, Iterable<LocationSummary>> availableContent = Maps.newHashMap();

        for (ContentProtos.ItemAndLocationSummary itemAndBroadcastRef : itemAndLocationSummaries) {
            ItemRef itemRef = (ItemRef) contentRefSerializer.deserialize(itemAndBroadcastRef.getItem());
            ImmutableList.Builder<LocationSummary> locationSummaries = ImmutableList.builder();

            for (int i = 0; i < itemAndBroadcastRef.getLocationSummaryCount(); i++) {
                CommonProtos.LocationSummary locationSummaryProto = itemAndBroadcastRef.getLocationSummary(
                        i);

                locationSummaries.add(
                        locationSummarySerializer.deserialize(locationSummaryProto)
                );
            }
            availableContent.put(
                    itemRef,
                    locationSummaries.build()
            );
        }

        return ImmutableMap.copyOf(availableContent);
    }
}
