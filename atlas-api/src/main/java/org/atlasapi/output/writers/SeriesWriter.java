package org.atlasapi.output.writers;

import java.io.IOException;

import javax.annotation.Nonnull;

import org.atlasapi.content.SeriesRef;
import org.atlasapi.output.EntityListWriter;
import org.atlasapi.output.FieldWriter;
import org.atlasapi.output.OutputContext;

import com.metabroadcast.common.ids.NumberToShortStringCodec;
import com.metabroadcast.common.ids.SubstitutionTableNumberCodec;

public class SeriesWriter implements EntityListWriter<SeriesRef> {


    private final NumberToShortStringCodec idCodec = SubstitutionTableNumberCodec.lowerCaseOnly();

    @Override public void write(@Nonnull SeriesRef entity, @Nonnull FieldWriter writer,
            @Nonnull OutputContext ctxt) throws IOException {
        writer.writeField("id", idCodec.encode(entity.getId().toBigInteger()));
        writer.writeField("series_number", entity.getSeriesNumber());
        writer.writeField("type", entity.getContentType());
    }

    @Override public String fieldName(SeriesRef entity) {
        return "series";
    }

    @Override public String listName() {
        return "series";
    }
}
