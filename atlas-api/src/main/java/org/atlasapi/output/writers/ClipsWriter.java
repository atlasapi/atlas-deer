package org.atlasapi.output.writers;

import java.io.IOException;

import javax.annotation.Nonnull;

import org.atlasapi.content.Clip;
import org.atlasapi.output.EntityListWriter;
import org.atlasapi.output.FieldWriter;
import org.atlasapi.output.OutputContext;

public class ClipsWriter implements EntityListWriter<Clip> {

    @Nonnull
    @Override
    public String listName() {
        return "clips";
    }

    @Override
    public void write(@Nonnull Clip entity, @Nonnull FieldWriter writer,
            @Nonnull OutputContext ctxt) throws IOException {
        writer.writeField("uri", entity.getCanonicalUri());
        writer.writeField("title", entity.getTitle());
        writer.writeField("clipOf", entity.getClipOf());

    }

    @Nonnull
    @Override
    public String fieldName(Clip entity) {
        return "clip";
    }
}
