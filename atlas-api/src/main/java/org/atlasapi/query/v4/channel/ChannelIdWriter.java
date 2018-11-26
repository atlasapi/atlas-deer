package org.atlasapi.query.v4.channel;

import java.io.IOException;

import javax.annotation.Nonnull;

import org.atlasapi.channel.Channel;
import org.atlasapi.channel.ChannelNumbering;
import org.atlasapi.channel.ResolvedChannel;
import org.atlasapi.output.EntityListWriter;
import org.atlasapi.output.FieldWriter;
import org.atlasapi.output.OutputContext;

import com.metabroadcast.common.ids.NumberToShortStringCodec;
import com.metabroadcast.common.ids.SubstitutionTableNumberCodec;

import static com.google.common.base.Preconditions.checkNotNull;

public class ChannelIdWriter implements EntityListWriter<ChannelNumbering> {

    private static final NumberToShortStringCodec idCodec = SubstitutionTableNumberCodec.lowerCaseOnly();

    private final String listName;
    private final String fieldName;

    protected ChannelIdWriter(String listName, String fieldName) {
        this.listName = checkNotNull(listName);
        this.fieldName = checkNotNull(fieldName);
    }

    public static ChannelIdWriter create(String listName, String fieldName) {
        return new ChannelIdWriter(listName, fieldName);
    }

    @Nonnull
    @Override
    public String listName() {
        return listName;
    }

    @Override
    public void write(
            @Nonnull ChannelNumbering entity,
            @Nonnull FieldWriter format,
            @Nonnull OutputContext ctxt
    ) throws IOException {
        format.writeField("id", idCodec.encode(entity.getChannel().getId().toBigInteger()));
    }

    @Override
    public String fieldName(ChannelNumbering entity) {
        return fieldName;
    }
}
