package org.atlasapi.application.writers;

import java.io.IOException;

import org.atlasapi.application.sources.SourceIdCodec;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.output.EntityListWriter;
import org.atlasapi.output.EntityWriter;
import org.atlasapi.output.FieldWriter;
import org.atlasapi.output.OutputContext;
import org.atlasapi.output.writers.CountryWriter;

import com.metabroadcast.common.intl.Country;

public class SourceWithIdWriter implements
        EntityListWriter<Publisher> {

    private final SourceIdCodec sourceIdCodec;
    private final String fieldName;
    private final String listName;
    private final EntityWriter<Country> countryWriter = new CountryWriter("countries", "country");

    public SourceWithIdWriter(SourceIdCodec sourceIdCodec, String fieldName, String listName) {
        this.sourceIdCodec = sourceIdCodec;
        this.fieldName = fieldName;
        this.listName = listName;
    }

    @Override
    public void write(
            Publisher entity,
            FieldWriter writer,
            OutputContext ctxt
    ) throws IOException {
        writer.writeField("id", sourceIdCodec.encode(entity));
        writer.writeField("key", entity.key());
        writer.writeField("name", entity.title());
        writer.writeObject(countryWriter, entity.country(), ctxt);
    }

    @Override
    public String fieldName(Publisher entity) {
        return fieldName;
    }

    @Override
    public String listName() {
        return listName;
    }

}
