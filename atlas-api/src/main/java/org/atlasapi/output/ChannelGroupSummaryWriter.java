package org.atlasapi.output;

import java.io.IOException;

import org.atlasapi.channel.ChannelGroupSummary;
import org.atlasapi.output.writers.AliasWriter;

import com.metabroadcast.common.ids.NumberToShortStringCodec;

import static com.google.common.base.Preconditions.checkNotNull;

public class ChannelGroupSummaryWriter implements EntityListWriter<ChannelGroupSummary> {

    private static final AliasWriter ALIAS_WRITER = new AliasWriter();
    private final NumberToShortStringCodec codec;

    public ChannelGroupSummaryWriter(NumberToShortStringCodec codec) {
        this.codec = checkNotNull(codec);
    }

    @Override
    public String listName() {
        return "channel_groups";
    }

    @Override
    public void write(ChannelGroupSummary entity, FieldWriter writer, OutputContext ctxt)
            throws IOException {
        writer.writeField("id", codec.encode(entity.getId().toBigInteger()));
        writer.writeField("title", entity.getTitle());
        writer.writeField("type", entity.getType());
        writer.writeList(ALIAS_WRITER, entity.getAliases(), ctxt);
    }

    @Override
    public String fieldName(ChannelGroupSummary entity) {
        return "channel_group";
    }
}
