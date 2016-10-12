package org.atlasapi.output.writers;

import java.io.IOException;

import javax.annotation.Nonnull;

import org.atlasapi.entity.Identified;
import org.atlasapi.output.EntityWriter;
import org.atlasapi.output.FieldWriter;
import org.atlasapi.output.OutputContext;

import com.metabroadcast.common.ids.NumberToShortStringCodec;
import com.metabroadcast.common.ids.SubstitutionTableNumberCodec;

public class IdSummaryWriter implements EntityWriter<Identified> {

    private final NumberToShortStringCodec codec;

    private IdSummaryWriter() {
        this.codec = SubstitutionTableNumberCodec.lowerCaseOnly();
    }

    public static IdSummaryWriter create() {
        return new IdSummaryWriter();
    }

    @Override
    public void write(
            @Nonnull Identified entity,
            @Nonnull FieldWriter writer,
            @Nonnull OutputContext ctxt
    ) throws IOException {
        //noinspection ConstantConditions
        String id = entity.getId() != null
                    ? codec.encode(entity.getId().toBigInteger())
                    : null;

        writer.writeField("id", id);
    }

    @Nonnull
    @Override
    public String fieldName(Identified entity) {
        return "id";
    }
}
