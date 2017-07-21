package org.atlasapi.output.annotation;

import java.io.IOException;

import org.atlasapi.content.Content;
import org.atlasapi.content.Item;
import org.atlasapi.content.Series;
import org.atlasapi.output.FieldWriter;
import org.atlasapi.output.OutputContext;

import com.metabroadcast.common.ids.NumberToShortStringCodec;

import static com.google.common.base.Preconditions.checkNotNull;

public class BrandReferenceAnnotation extends OutputAnnotation<Content> {

    private static final String CONTAINER_FIELD = "container";

    private final ResourceRefWriter brandRefWriter;

    public BrandReferenceAnnotation(NumberToShortStringCodec idCodec) {
        super();
        brandRefWriter = new ResourceRefWriter(CONTAINER_FIELD, checkNotNull(idCodec));
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
        } else if (content instanceof Series) {
            Series series = (Series) content;
            if (series.getBrandRef() == null) {
                writer.writeField(CONTAINER_FIELD, null);
            } else {
                writer.writeObject(brandRefWriter, series.getBrandRef(), ctxt);
            }
        }
    }

}
