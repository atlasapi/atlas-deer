package org.atlasapi.output.writers;

import com.metabroadcast.common.ids.NumberToShortStringCodec;
import com.metabroadcast.common.ids.SubstitutionTableNumberCodec;
import org.atlasapi.channel.ChannelGroup;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.output.EntityListWriter;
import org.atlasapi.output.EntityWriter;
import org.atlasapi.output.FieldWriter;
import org.atlasapi.output.OutputContext;

import javax.annotation.Nonnull;
import java.io.IOException;

import static org.atlasapi.output.writers.SourceWriter.sourceWriter;

public class ChannelGroupWriter implements EntityListWriter<ChannelGroup> {

    private static final EntityWriter<Publisher> SOURCE_WRITER = sourceWriter("source");
    private static final AliasWriter ALIAS_WRITER = new AliasWriter();
    private static final CountryWriter COUNTRY_WRITER = new CountryWriter("available_countries", "available_country");

    private final String listName;
    private final String fieldName;
    private final NumberToShortStringCodec idCode = SubstitutionTableNumberCodec.lowerCaseOnly();

    public ChannelGroupWriter(String listName, String fieldName) {
        this.listName = listName;
        this.fieldName = fieldName;
    }


    @Nonnull
    @Override
    public String listName() {
        return listName;
    }

    @Override
    public void write(@Nonnull ChannelGroup entity, @Nonnull FieldWriter format, @Nonnull OutputContext ctxt) throws IOException {
        format.writeField("id", idCode.encode(entity.getId().toBigInteger()));
        format.writeField("title", entity.getTitle());
        format.writeField("uri", entity.getCanonicalUri());
        format.writeField("type", entity.getType());
        format.writeObject(SOURCE_WRITER, entity.getPublisher(), ctxt);
        format.writeList(ALIAS_WRITER, entity.getAliases(), ctxt);
        format.writeList(COUNTRY_WRITER, entity.getAvailableCountries(), ctxt);

    }

    @Nonnull
    @Override
    public String fieldName(ChannelGroup entity) {
        return fieldName;
    }
}
