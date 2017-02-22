package org.atlasapi.output.writers;

import com.metabroadcast.common.ids.NumberToShortStringCodec;
import org.atlasapi.content.ChannelVariantRef;
import org.atlasapi.output.EntityListWriter;
import org.atlasapi.output.FieldWriter;
import org.atlasapi.output.OutputContext;

import java.io.IOException;

public class ChannelVariantRefWriter implements EntityListWriter<ChannelVariantRef> {

    private final String fieldName;
    private final String listName;
    private final NumberToShortStringCodec codec;

    private ChannelVariantRefWriter(
            String fieldName,
            String listName,
            NumberToShortStringCodec codec
    ) {
        this.fieldName = fieldName;
        this.listName = listName;
        this.codec = codec;
    }

    public static ChannelVariantRefWriter create(
            String fieldName,
            String listName,
            NumberToShortStringCodec codec
    ) {
        return new ChannelVariantRefWriter(fieldName, listName, codec);
    }

    @Override
    public void write(ChannelVariantRef entity, FieldWriter writer, OutputContext ctxt)
            throws IOException {
        writer.writeField("id", codec.encode(entity.getId().toBigInteger()));
        writer.writeField("title", entity.getTitle());
    }

    @Override
    public String fieldName(ChannelVariantRef entity) {
        return fieldName;
    }

    @Override
    public String listName() {
        return listName;
    }

}
