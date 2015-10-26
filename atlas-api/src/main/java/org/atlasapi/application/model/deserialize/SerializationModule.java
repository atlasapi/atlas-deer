package org.atlasapi.application.model.deserialize;

import static com.google.gson.FieldNamingPolicy.LOWER_CASE_WITH_UNDERSCORES;

import org.atlasapi.application.SourceReadEntry;
import org.atlasapi.application.users.Role;
import org.atlasapi.entity.Id;
import org.atlasapi.input.GsonModelReader;
import org.atlasapi.input.ModelReader;
import org.atlasapi.media.entity.Publisher;
import org.joda.time.DateTime;
import org.springframework.context.annotation.Bean;

import com.google.common.base.Optional;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonDeserializer;
import com.google.gson.reflect.TypeToken;
import com.metabroadcast.common.ids.NumberToShortStringCodec;
import com.metabroadcast.common.ids.SubstitutionTableNumberCodec;
import com.metabroadcast.common.webapp.serializers.JodaDateTimeSerializer;

public class SerializationModule {
    private final NumberToShortStringCodec idCodec = SubstitutionTableNumberCodec.lowerCaseOnly();
    private final JsonDeserializer<Id> idDeserializer = new IdDeserializer(idCodec);
    private final JsonDeserializer<DateTime> datetimeDeserializer = new JodaDateTimeSerializer();
    private final JsonDeserializer<SourceReadEntry> readsDeserializer = new SourceReadEntryDeserializer();
    private final JsonDeserializer<Publisher> publisherDeserializer = new PublisherDeserializer();
    private final JsonDeserializer<Role> roleDeserializer = new RoleDeserializer();

    @Bean public Gson gson() {
        return new GsonBuilder()
                .registerTypeAdapter(DateTime.class, datetimeDeserializer)
                .registerTypeAdapter(Id.class, idDeserializer)
                .registerTypeAdapter(SourceReadEntry.class, readsDeserializer)
                .registerTypeAdapter(Publisher.class, publisherDeserializer)
                .registerTypeAdapter(Role.class, roleDeserializer)
                .registerTypeAdapter(new TypeToken<Optional<DateTime>>(){}.getType(), new OptionalDeserializer())
                .setFieldNamingPolicy(LOWER_CASE_WITH_UNDERSCORES)
                .create();
    }

    @Bean protected ModelReader gsonModelReader() {
        return new GsonModelReader(gson());
    }
}
