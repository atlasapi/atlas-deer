package org.atlasapi.query.annotation;

import com.metabroadcast.common.stream.MoreCollectors;
import org.atlasapi.annotation.Annotation;
import org.atlasapi.content.Described;
import org.atlasapi.content.Image;
import org.atlasapi.output.EntityListWriter;
import org.atlasapi.output.FieldWriter;
import org.atlasapi.output.OutputContext;
import org.atlasapi.output.annotation.OutputAnnotation;
import org.atlasapi.output.writers.ImageListWriter;

import java.io.IOException;
import java.util.Set;

import static org.atlasapi.util.QueryUtils.contextHasAnnotation;

public class ImagesAnnotation extends OutputAnnotation<Described> {

    private final EntityListWriter<Image> imageWriter = new ImageListWriter();

    @Override
    public void write(Described entity, FieldWriter writer, OutputContext ctxt) throws IOException {
        Set<Image> images = contextHasAnnotation(ctxt, Annotation.UNAVAILABLE_IMAGES) ?
                entity.getImages() :
                entity.getImages().stream()
                        .filter(Image.IS_AVAILABLE::apply)
                        .collect(MoreCollectors.toImmutableSet());
        writer.writeList(imageWriter, images, ctxt);
    }

}
