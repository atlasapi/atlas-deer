package org.atlasapi.output.writers;

import java.io.IOException;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Futures;
import org.atlasapi.content.Content;
import org.atlasapi.content.ContentResolver;
import org.atlasapi.content.Series;
import org.atlasapi.content.SeriesRef;
import org.atlasapi.entity.util.Resolved;
import org.atlasapi.output.EntityListWriter;
import org.atlasapi.output.FieldWriter;
import org.atlasapi.output.OutputContext;

import com.metabroadcast.common.ids.NumberToShortStringCodec;
import com.metabroadcast.common.ids.SubstitutionTableNumberCodec;

import static com.google.common.base.Preconditions.checkNotNull;

public class SeriesWriter implements EntityListWriter<SeriesRef> {


    private final NumberToShortStringCodec idCodec = SubstitutionTableNumberCodec.lowerCaseOnly();
    private final ContentResolver contentResolver;

    public SeriesWriter(ContentResolver contentResolver) {
        this.contentResolver = checkNotNull(contentResolver);
    }

    @Override public void write(@Nonnull SeriesRef entity, @Nonnull FieldWriter writer,
            @Nonnull OutputContext ctxt) throws IOException {

        writer.writeField("id", idCodec.encode(entity.getId().toBigInteger()));
        writer.writeField("series_number", entity.getSeriesNumber());
        writer.writeField("type", entity.getContentType());

        Resolved<Content> resolved =
                Futures.get(contentResolver.resolveIds(ImmutableList.of(entity.getId())), IOException.class);
        Series series = (Series) resolved.getResources().first().get();
        writer.writeList("release_years", "release_year", series.getReleaseYears(), ctxt);
    }

    @Override public String fieldName(SeriesRef entity) {
        return "series";
    }

    @Override public String listName() {
        return "series";
    }
}
