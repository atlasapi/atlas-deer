package org.atlasapi.content;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import org.atlasapi.entity.Id;
import org.atlasapi.serialization.protobuf.CommonProtos;
import org.atlasapi.serialization.protobuf.ContentProtos;
import org.joda.time.Interval;

import java.util.Map;

import static org.atlasapi.entity.ProtoBufUtils.deserializeDateTime;
import static org.atlasapi.entity.ProtoBufUtils.serializeDateTime;

public class ItemAndBroadcastRefSerializer {


    public ContentProtos.ItemAndBroadcastRef.Builder serialize(ItemRef itemRef, BroadcastRef broadcastRef) {

        ContentRefSerializer contentRefSerializer = new ContentRefSerializer(itemRef.getSource());
        CommonProtos.Reference.Builder itemRefBuilder = contentRefSerializer.serialize(itemRef);

        ContentProtos.ItemAndBroadcastRef.Builder builder = ContentProtos.ItemAndBroadcastRef.newBuilder();

        CommonProtos.BroadcastRef.Builder broadcastRefBuilder = CommonProtos.BroadcastRef.newBuilder();

        CommonProtos.Identification.Builder id = CommonProtos.Identification.newBuilder();
        id.setId(broadcastRef.getChannelId().longValue());

        broadcastRefBuilder.setSourceId(broadcastRef.getSourceId());
        broadcastRefBuilder.setChannelId(id);
        broadcastRefBuilder.setTransmissionTime(serializeDateTime(broadcastRef.getTransmissionInterval().getStart()));
        broadcastRefBuilder.setTransmissionEndTime(serializeDateTime(broadcastRef.getTransmissionInterval().getEnd()));

        builder.setBroadcast(broadcastRefBuilder);
        builder.setItem(itemRefBuilder);
        return builder;
    }


    public Map<ItemRef, Iterable<BroadcastRef>> deserialize(Iterable<ContentProtos.ItemAndBroadcastRef> itemAndBroadcastRefs) {
        ContentRefSerializer contentRefSerializer = new ContentRefSerializer(null);
        Map<ItemRef, Iterable<BroadcastRef>> upcomingContent = Maps.newHashMap();

        for (ContentProtos.ItemAndBroadcastRef itemAndBroadcastRef : itemAndBroadcastRefs) {
            ItemRef itemRef = (ItemRef) contentRefSerializer.deserialize(itemAndBroadcastRef.getItem());
            BroadcastRef broadcastRef = new BroadcastRef(
                    itemAndBroadcastRef.getBroadcast().getSourceId(),
                    Id.valueOf(itemAndBroadcastRef.getBroadcast().getChannelId().getId()),
                    new Interval(
                            deserializeDateTime(itemAndBroadcastRef.getBroadcast().getTransmissionTime()),
                            deserializeDateTime(itemAndBroadcastRef.getBroadcast().getTransmissionEndTime())
                    )
            );

            ImmutableList<BroadcastRef> broadcastRefs = ImmutableList.<BroadcastRef>builder()
                    .addAll(upcomingContent.getOrDefault(itemRef, ImmutableList.<BroadcastRef>of()))
                    .add(broadcastRef)
                    .build();


            upcomingContent.put(
                    itemRef,
                    broadcastRefs

            );
        }

        return ImmutableMap.copyOf(upcomingContent);
    }

}
