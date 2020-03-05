package org.atlasapi.output.writers;

import java.io.IOException;

import javax.annotation.Nonnull;

import org.atlasapi.content.LocalizedTitle;
import org.atlasapi.output.EntityListWriter;
import org.atlasapi.output.FieldWriter;
import org.atlasapi.output.OutputContext;

public class LocalizedTitlesWriter implements EntityListWriter<LocalizedTitle> {

    @Override
    public void write(@Nonnull LocalizedTitle entity, @Nonnull FieldWriter writer, @Nonnull OutputContext ctxt)
            throws IOException {

        writer.writeField("value", entity.getTitle());
        writer.writeField("locale", entity.getLanguageTag());
    }

    @Override
    public String listName() {
        return "localizedTitles";
    }

    @Override
    public String fieldName(LocalizedTitle entity) {
        return "localizedTitle";
    }

}
