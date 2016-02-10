package org.atlasapi.output.writers;

import java.io.IOException;

import org.atlasapi.content.Image;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.output.EntityListWriter;
import org.atlasapi.output.EntityWriter;
import org.atlasapi.output.FieldWriter;
import org.atlasapi.output.OutputContext;

public class ImageListWriter implements EntityListWriter<Image> {

    private final EntityWriter<Publisher> sourceWriter = SourceWriter.sourceWriter("source");

    @Override
    public void write(Image entity, FieldWriter writer, OutputContext ctxt) throws IOException {
        writer.writeField("uri", entity.getCanonicalUri());
        writer.writeField("mime_type", entity.getMimeType());

        Image.Type type = entity.getType();
        writer.writeField("type", type != null ? entity.getType().getName() : null);

        Image.Color color = entity.getColor();
        writer.writeField("color", color != null ? entity.getColor().getName() : null);

        Image.Theme theme = entity.getTheme();
        writer.writeField("theme", theme != null ? entity.getTheme().getName() : null);

        Image.AspectRatio aspectRatio = entity.getAspectRatio();
        writer.writeField(
                "aspect_ratio",
                aspectRatio != null ? entity.getAspectRatio().getName() : null
        );

        writer.writeField("availability_start", entity.getAvailabilityStart());
        writer.writeField("availability_end", entity.getAvailabilityEnd());
        writer.writeField("width", entity.getWidth());
        writer.writeField("height", entity.getHeight());

        writer.writeField("hasTitleArt", entity.hasTitleArt());
        writer.writeObject(sourceWriter, entity.getSource(), ctxt);

    }

    @Override
    public String fieldName(Image entity) {
        return "image";
    }

    @Override
    public String listName() {
        return "images";
    }

}
