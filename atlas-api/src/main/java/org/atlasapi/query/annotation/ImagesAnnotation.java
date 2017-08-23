package org.atlasapi.query.annotation;

import java.io.IOException;

import org.atlasapi.channel.ResolvedChannelGroup;
import org.atlasapi.content.Described;
import org.atlasapi.content.Image;
import org.atlasapi.content.ResolvedContent;
import org.atlasapi.output.EntityListWriter;
import org.atlasapi.output.FieldWriter;
import org.atlasapi.output.OutputContext;
import org.atlasapi.output.annotation.OutputAnnotation;
import org.atlasapi.output.writers.ImageListWriter;

public class ImagesAnnotation extends OutputAnnotation<Described, ResolvedContent> {

    private final EntityListWriter<Image> imageWriter = new ImageListWriter();

    @Override
    public void write(ResolvedContent entity, FieldWriter writer, OutputContext ctxt) throws IOException {
        writer.writeList(imageWriter, entity.getDescribed().getImages(), ctxt);
    }

}
