package org.atlasapi.content;

import java.util.Map;

import org.atlasapi.entity.DateTimeSerializer;
import org.atlasapi.entity.Id;
import org.atlasapi.serialization.protobuf.CommonProtos;
import org.atlasapi.serialization.protobuf.ContentProtos;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import org.joda.time.Interval;

public class ItemAndBroadcastRefSerializer {

    private final DateTimeSerializer dateTimeSerializer = new DateTimeSerializer();

    public ContentProtos.ItemAndBroadcastRef.Builder serialize(ItemRef itemRef,
            Iterable<BroadcastRef> broadcastRefs) {

        ContentRefSerializer contentRefSerializer = new ContentRefSerializer(itemRef.getSource());
        CommonProtos.Reference.Builder itemRefBuilder = contentRefSerializer.serialize(itemRef);

        ContentProtos.ItemAndBroadcastRef.Builder builder = ContentProtos.ItemAndBroadcastRef.newBuilder();

        for (BroadcastRef broadcastRef : broadcastRefs) {
            CommonProtos.Identification.Builder id = CommonProtos.Identification.newBuilder();
            id.setId(broadcastRef.getChannelId().longValue());

            CommonProtos.BroadcastRef.Builder broadcastRefBuilder = CommonProtos.BroadcastRef.newBuilder();
            broadcastRefBuilder.setSourceId(broadcastRef.getSourceId());
            broadcastRefBuilder.setChannelId(id);
            broadcastRefBuilder.setTransmissionTime(dateTimeSerializer.serialize(broadcastRef.getTransmissionInterval()
                    .getStart()));
            broadcastRefBuilder.setTransmissionEndTime(dateTimeSerializer.serialize(
                    broadcastRef
                            .getTransmissionInterval()
                            .getEnd()));

            builder.addBroadcast(broadcastRefBuilder);
        }

        builder.setItem(itemRefBuilder);
        return builder;
    }

    public Map<ItemRef, Iterable<BroadcastRef>> deserialize(
            Iterable<ContentProtos.ItemAndBroadcastRef> itemAndBroadcastRefs) {
        ContentRefSerializer contentRefSerializer = new ContentRefSerializer(null);
        Map<ItemRef, Iterable<BroadcastRef>> upcomingContent = Maps.newHashMap();

        for (ContentProtos.ItemAndBroadcastRef itemAndBroadcastRef : itemAndBroadcastRefs) {
            ItemRef itemRef = (ItemRef) contentRefSerializer.deserialize(itemAndBroadcastRef.getItem());
            ImmutableList.Builder<BroadcastRef> broadcastRefs = ImmutableList.<BroadcastRef>builder();

            for (int i = 0; i < itemAndBroadcastRef.getBroadcastCount(); i++) {
                CommonProtos.BroadcastRef broadcastProto = itemAndBroadcastRef.getBroadcast(i);

                broadcastRefs.add(
                        new BroadcastRef(
                                broadcastProto.getSourceId(),
                                Id.valueOf(broadcastProto.getChannelId().getId()),
                                new Interval(
                                        dateTimeSerializer.deserialize(broadcastProto.getTransmissionTime()),
                                        dateTimeSerializer.deserialize(broadcastProto.getTransmissionEndTime())
                                )
                        )
                );
            }
            upcomingContent.put(
                    itemRef,
                    broadcastRefs.build()
            );
        }

        return ImmutableMap.copyOf(upcomingContent);
    }

}
