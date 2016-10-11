package org.atlasapi.output.annotation;

import java.io.IOException;

import org.atlasapi.entity.Identified;
import org.atlasapi.output.FieldWriter;
import org.atlasapi.output.OutputContext;

import com.metabroadcast.common.ids.NumberToShortStringCodec;

public class IdentificationSummaryAnnotation extends OutputAnnotation<Identified> {

    private final NumberToShortStringCodec codec;

    public IdentificationSummaryAnnotation(NumberToShortStringCodec codec) {
        super();
        this.codec = codec;
    }

    @Override
    public void write(Identified entity, FieldWriter formatter, OutputContext ctxt)
            throws IOException {
        formatter.writeField("id", encodedIdOrNull(entity));
    }

    private String encodedIdOrNull(Identified entity) {
        return entity.getId() != null ? codec.encode(entity.getId().toBigInteger())
                                      : null;
    }
}
