package org.atlasapi.output.writers;

import java.io.IOException;
import static com.google.common.base.Preconditions.checkNotNull;

import javax.annotation.Nonnull;

import org.atlasapi.content.Clip;
import org.atlasapi.content.ResolvedContent;
import org.atlasapi.output.EntityListWriter;
import org.atlasapi.output.FieldWriter;
import org.atlasapi.output.OutputContext;
import org.atlasapi.output.annotation.LocationsAnnotation;

public class ClipsWriter implements EntityListWriter<Clip> {

    private final LocationsAnnotation locationsAnnotation;

    public ClipsWriter(LocationsAnnotation locationsAnnotation) {
        this.locationsAnnotation = checkNotNull(locationsAnnotation);
    }

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

        locationsAnnotation.write(ResolvedContent.wrap(entity), writer, ctxt);
    }

    @Nonnull
    @Override
    public String fieldName(Clip entity) {
        return "clip";
    }
}
