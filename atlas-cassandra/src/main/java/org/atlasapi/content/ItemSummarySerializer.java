package org.atlasapi.content;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.atlasapi.serialization.protobuf.CommonProtos;
import org.atlasapi.serialization.protobuf.ContentProtos;

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class ItemSummarySerializer {

    private static final Map<Class<? extends ItemSummary>, String> CLASS_TO_TYPE = ImmutableMap.<Class<? extends ItemSummary>, String>builder()
            .put(ItemSummary.class, "item")
            .put(EpisodeSummary.class, "episode")
            .build();

    private static final Map<String, Function<ContentProtos.ItemSummary, ? extends ItemSummary>> TYPE_TO_CONSTRUCTOR = ImmutableMap.<String, Function<ContentProtos.ItemSummary, ? extends ItemSummary>>builder()
            .put(
                    "episode",
                    proto -> new EpisodeSummary(
                            (ItemRef) new ContentRefSerializer(null).deserialize(proto.getItemRef()),
                            proto.getTitle(),
                            proto.hasDescription() ? proto.getDescription() : null,
                            proto.hasImage() ? proto.getImage() : null,
                            proto.hasEpisodeNumber() ? proto.getEpisodeNumber() : null
                    )
            )
            .put("item", proto -> new ItemSummary(
                            (ItemRef) new ContentRefSerializer(null).deserialize(proto.getItemRef()),
                            proto.getTitle(),
                            proto.hasDescription() ? proto.getDescription() : null,
                            proto.hasImage() ? proto.getImage() : null
                    )
            )
            .build();

    public ContentProtos.ItemSummary.Builder serialize(ItemSummary itemSummary) {

        ContentRefSerializer contentRefSerializer = new ContentRefSerializer(itemSummary.getItemRef().getSource());
        CommonProtos.Reference.Builder itemRefBuilder = contentRefSerializer.serialize(itemSummary.getItemRef());
        ContentProtos.ItemSummary.Builder builder = ContentProtos.ItemSummary.newBuilder();

        builder.setItemRef(itemRefBuilder);
        builder.setTitle(itemSummary.getTitle());
        builder.setType(CLASS_TO_TYPE.get(itemSummary.getClass()));
        if(itemSummary.getDescription().isPresent()) {
            builder.setDescription(itemSummary.getDescription().get());
        }
        if (itemSummary.getImage().isPresent()) {
            builder.setImage(
                    itemSummary.getImage().get()
            );
        }
        if (itemSummary instanceof EpisodeSummary && ((EpisodeSummary) itemSummary).getEpisodeNumber().isPresent()) {
            builder.setEpisodeNumber(((EpisodeSummary) itemSummary).getEpisodeNumber().get());
        }
        return builder;
    }

    public List<ItemSummary> deserialize (Iterable<ContentProtos.ItemSummary> itemSummariesProtos) {

        return StreamSupport.stream(itemSummariesProtos.spliterator(), false)
                .map(proto -> TYPE_TO_CONSTRUCTOR.get(proto.getType()).apply(proto))
                .collect(Collectors.<ItemSummary>toList());


    }
}
