package org.atlasapi.output.annotation;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.IOException;

import org.atlasapi.content.Content;
import org.atlasapi.content.Item;
import org.atlasapi.output.FieldWriter;
import org.atlasapi.output.OutputContext;

import com.metabroadcast.common.ids.NumberToShortStringCodec;

public class BrandReferenceAnnotation extends OutputAnnotation<Content> {

    private static final String CONTAINER_FIELD = "container";

    private final ContainerRefWriter brandRefWriter;

    public BrandReferenceAnnotation(NumberToShortStringCodec idCodec) {
        super();
        brandRefWriter = new ContainerRefWriter(CONTAINER_FIELD, checkNotNull(idCodec));
    }

    @Override
    public void write(Content content, FieldWriter writer, OutputContext ctxt) throws IOException {
        if (content instanceof Item) {
            Item item = (Item) content;
            if (item.getContainerRef() == null) {
                writer.writeField(CONTAINER_FIELD, null);
            } else {
                writer.writeObject(brandRefWriter, item.getContainerRef(), ctxt);
            }
        }
    }

}
