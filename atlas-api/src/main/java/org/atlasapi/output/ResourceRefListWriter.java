package org.atlasapi.output;

import java.io.IOException;
import java.math.BigInteger;

import javax.annotation.Nonnull;

import org.atlasapi.entity.ResourceRef;

import com.metabroadcast.common.ids.NumberToShortStringCodec;

import static com.google.common.base.Preconditions.checkNotNull;

public class ResourceRefListWriter implements EntityListWriter<ResourceRef> {

    private final String listName;
    private final String fieldsName;
    private final NumberToShortStringCodec idCodec;

    public ResourceRefListWriter(String listName, String fieldsName,
            NumberToShortStringCodec idCodec) {
        this.listName = checkNotNull(listName);
        this.fieldsName = checkNotNull(fieldsName);
        this.idCodec = checkNotNull(idCodec);
    }

    @Nonnull
    @Override
    public String listName() {
        return listName;
    }

    @Override
    public void write(@Nonnull ResourceRef entity, @Nonnull FieldWriter writer,
            @Nonnull OutputContext ctxt) throws IOException {
        BigInteger id = entity.getId().toBigInteger();
        writer.writeField("id", idCodec.encode(id));
    }

    @Nonnull
    @Override
    public String fieldName(ResourceRef entity) {
        return fieldsName;
    }
}
