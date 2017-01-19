package org.atlasapi.application.www;

import javax.servlet.http.HttpServletRequest;

import org.atlasapi.AtlasPersistenceModule;
import org.atlasapi.LicenseModule;
import org.atlasapi.annotation.Annotation;
import org.atlasapi.application.ApplicationPersistenceModule;
import org.atlasapi.application.SourceReadEntry;
import org.atlasapi.application.ApiKeyApplicationFetcher;
import org.atlasapi.application.ApplicationFetcher;
import org.atlasapi.application.model.deserialize.IdDeserializer;
import org.atlasapi.application.model.deserialize.OptionalDeserializer;
import org.atlasapi.application.model.deserialize.PublisherDeserializer;
import org.atlasapi.application.model.deserialize.SourceReadEntryDeserializer;
import org.atlasapi.entity.Id;
import org.atlasapi.input.GsonModelReader;
import org.atlasapi.input.ModelReader;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.output.EntityWriter;
import org.atlasapi.output.writers.RequestWriter;
import org.atlasapi.query.annotation.ResourceAnnotationIndex;
import org.atlasapi.query.common.Resource;

import com.metabroadcast.common.http.HttpClients;
import com.metabroadcast.common.http.SimpleHttpClient;
import com.metabroadcast.common.ids.NumberToShortStringCodec;
import com.metabroadcast.common.ids.SubstitutionTableNumberCodec;
import com.metabroadcast.common.query.Selection;
import com.metabroadcast.common.query.Selection.SelectionBuilder;
import com.metabroadcast.common.social.user.FixedAppIdUserRefBuilder;
import com.metabroadcast.common.webapp.serializers.JodaDateTimeSerializer;

import com.google.common.base.Optional;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonDeserializer;
import com.google.gson.reflect.TypeToken;
import org.joda.time.DateTime;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.web.servlet.mvc.annotation.DefaultAnnotationHandlerMapping;

import static com.google.gson.FieldNamingPolicy.LOWER_CASE_WITH_UNDERSCORES;

@Configuration
@Import({
        AtlasPersistenceModule.class,
        ApplicationPersistenceModule.class,
        LicenseModule.class
})
public class ApplicationWebModule {

    private final NumberToShortStringCodec idCodec = SubstitutionTableNumberCodec.lowerCaseOnly();
    private final JsonDeserializer<Id> idDeserializer = new IdDeserializer(idCodec);
    private final JsonDeserializer<DateTime> datetimeDeserializer = new JodaDateTimeSerializer();
    private final JsonDeserializer<SourceReadEntry> readsDeserializer = new SourceReadEntryDeserializer();
    private final JsonDeserializer<Publisher> publisherDeserializer = new PublisherDeserializer();

    @Autowired AtlasPersistenceModule persistence;
    @Autowired ApplicationPersistenceModule appPersistence;

    @Autowired @Qualifier("licenseWriter") EntityWriter<Object> licenseWriter;

    private static final String APP_NAME = "atlas";

    @Value("${youtube.handling.service}") private String handlingService;

    private final Gson gson = new GsonBuilder()
            .registerTypeAdapter(DateTime.class, datetimeDeserializer)
            .registerTypeAdapter(Id.class, idDeserializer)
            .registerTypeAdapter(SourceReadEntry.class, readsDeserializer)
            .registerTypeAdapter(Publisher.class, publisherDeserializer)
            .registerTypeAdapter(new TypeToken<Optional<DateTime>>() {

            }.getType(), new OptionalDeserializer())
            .setFieldNamingPolicy(LOWER_CASE_WITH_UNDERSCORES)
            .create();

    @Bean
    protected ModelReader gsonModelReader() {
        return new GsonModelReader(gson);
    }

    @Bean
    ResourceAnnotationIndex applicationAnnotationIndex() {
        return ResourceAnnotationIndex.builder(Resource.APPLICATION, Annotation.all()).build();
    }

    @Bean
    SelectionBuilder selectionBuilder() {
        return Selection.builder().withDefaultLimit(50).withMaxLimit(100);
    }

    @Bean
    EntityWriter<HttpServletRequest> requestWriter() {
        return new RequestWriter();

    }

    public
    @Bean
    DefaultAnnotationHandlerMapping controllerMappings() {
        DefaultAnnotationHandlerMapping controllerClassNameHandlerMapping = new DefaultAnnotationHandlerMapping();
        return controllerClassNameHandlerMapping;
    }

    public
    @Bean
    ApplicationFetcher configFetcher() {
        return ApiKeyApplicationFetcher.create(appPersistence.applicationsClient());
    }

    public
    @Bean
    FixedAppIdUserRefBuilder userRefBuilder() {
        return new FixedAppIdUserRefBuilder(APP_NAME);
    }

    @Bean
    public SimpleHttpClient httpClient() {
        return HttpClients.webserviceClient();
    }

}
