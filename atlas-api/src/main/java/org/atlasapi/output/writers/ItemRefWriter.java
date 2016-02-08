package org.atlasapi.output.writers;

import java.io.IOException;
import java.util.Optional;

import javax.annotation.Nullable;

import org.atlasapi.content.ItemRef;
import org.atlasapi.output.EntityListWriter;
import org.atlasapi.output.FieldWriter;
import org.atlasapi.output.OutputContext;

import com.metabroadcast.common.ids.NumberToShortStringCodec;

import static com.google.common.base.Preconditions.checkNotNull;

public final class ItemRefWriter implements EntityListWriter<ItemRef> {

    private final String listName;
    private final Optional<String> fieldName;
    private final NumberToShortStringCodec idCodec;

    public ItemRefWriter(NumberToShortStringCodec idCodec, String listName) {
        this(idCodec, listName, null);
    }

    public ItemRefWriter(NumberToShortStringCodec idCodec, String listName,
            @Nullable String fieldName) {
        this.idCodec = checkNotNull(idCodec);
        this.listName = checkNotNull(listName);
        this.fieldName = Optional.ofNullable(fieldName);
    }

    @Override
    public void write(ItemRef entity, FieldWriter writer, OutputContext ctxt) throws IOException {
        writer.writeField("id", idCodec.encode(entity.getId().toBigInteger()));
        writer.writeField("type", entity.getContentType());
    }

    @Override
    public String listName() {
        return listName;
    }

    @Override
    public String fieldName(ItemRef entity) {
        return fieldName.orElse("content");
    }
}