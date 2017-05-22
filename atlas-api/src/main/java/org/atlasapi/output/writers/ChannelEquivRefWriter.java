package org.atlasapi.output.writers;

import com.metabroadcast.common.ids.NumberToShortStringCodec;
import org.atlasapi.channel.ChannelEquivRef;
import org.atlasapi.output.EntityListWriter;
import org.atlasapi.output.FieldWriter;
import org.atlasapi.output.OutputContext;

import java.io.IOException;

public class ChannelEquivRefWriter implements EntityListWriter<ChannelEquivRef> {

    private final String fieldName;
    private final String listName;
    private final NumberToShortStringCodec codec;

    private ChannelEquivRefWriter(
            String fieldName,
            String listName,
            NumberToShortStringCodec codec
    ) {
        this.fieldName = fieldName;
        this.listName = listName;
        this.codec = codec;
    }

    public static ChannelEquivRefWriter create(
            String fieldName,
            String listName,
            NumberToShortStringCodec codec
    ) {
        return new ChannelEquivRefWriter(fieldName, listName, codec);
    }

    @Override
    public void write(ChannelEquivRef entity, FieldWriter writer, OutputContext ctxt) throws IOException {
        writer.writeField("id", codec.encode(entity.getId().toBigInteger()));
        writer.writeField("uri", entity.getUri());
        writer.writeField("source", entity.getSource().key());
    }

    @Override
    public String fieldName(ChannelEquivRef entity) {
        return fieldName;
    }

    @Override
    public String listName() {
        return listName;
    }


}
