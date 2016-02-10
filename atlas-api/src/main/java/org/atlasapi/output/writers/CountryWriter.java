package org.atlasapi.output.writers;

import java.io.IOException;

import javax.annotation.Nonnull;

import org.atlasapi.output.EntityListWriter;
import org.atlasapi.output.FieldWriter;
import org.atlasapi.output.OutputContext;

import com.metabroadcast.common.intl.Country;

import static com.google.common.base.Preconditions.checkNotNull;

public class CountryWriter implements EntityListWriter<Country> {

    private final String listName;
    private final String fieldName;

    public CountryWriter(String listName, String fieldName) {
        this.listName = checkNotNull(listName);
        this.fieldName = checkNotNull(fieldName);
    }

    @Override
    public void write(Country entity, FieldWriter writer, OutputContext ctxt) throws IOException {
        writer.writeField("code", entity.code());
        writer.writeField("name", entity.getName());
    }

    @Override
    public String fieldName(Country entity) {
        return fieldName;
    }

    @Nonnull
    @Override
    public String listName() {
        return listName;
    }
}
