package org.atlasapi.model.translators;

import java.util.List;
import java.util.Map;

import org.atlasapi.application.Application;
import org.atlasapi.application.ApplicationCredentials;
import org.atlasapi.application.ApplicationSources;
import org.atlasapi.application.SourceReadEntry;
import org.atlasapi.application.v3.ApplicationConfiguration;
import org.atlasapi.entity.Id;
import org.atlasapi.media.entity.Publisher;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;


public class ApplicationModelTranslator implements Function<org.atlasapi.application.v3.Application, Application> {
    public static final Function<SourceReadEntry, Publisher> SOURCEREADENTRY_TO_PUBLISHER = new Function<SourceReadEntry, Publisher>() {

        @Override
        public Publisher apply(SourceReadEntry input) {
            return input.getPublisher();
        }
        
    };
    
    public Iterable<Application> transform(Iterable<org.atlasapi.application.v3.Application> inputs) {
        return Iterables.transform(inputs,this);
    }
    
    public Application apply(org.atlasapi.application.v3.Application input) {
         return Application.builder()
            .withId(Id.valueOf(input.getDeerId().longValue()))
            .withSlug(input.getSlug())
            .withTitle(input.getTitle())
            .withDescription(input.getDescription())
            .withCreated(input.getCreated())
            .withCredentials(transformCredentials3to4(input.getCredentials()))
            .withSources(transformConfiguration3to4(input.getConfiguration()))
            .withRevoked(input.isRevoked())
            .build();
    }
    
    private ApplicationCredentials transformCredentials3to4(org.atlasapi.application.v3.ApplicationCredentials input) {
        return new ApplicationCredentials(input.getApiKey());
    }
    
    private ApplicationSources transformConfiguration3to4(ApplicationConfiguration input) {
        List<SourceReadEntry> reads;
        if (input.precedenceEnabled()) {
            reads = asOrderedList(input.sourceStatuses(), input.precedence());
        } else {
            reads = asOrderedList(input.sourceStatuses(), input.sourceStatuses().keySet());
        }
        return ApplicationSources.builder()
                .withPrecedence(input.precedenceEnabled())
                .withReadableSources(reads)
                .withImagePrecedenceEnabled(input.imagePrecedenceEnabled())
                .withWritableSources(input.writableSources().asList())
                .withContentHierarchyPrecedence(input.contentHierarchyPrecedence().orNull())
                .build()
                .copyWithMissingSourcesPopulated();
    }
    
    private List<SourceReadEntry> asOrderedList(Map<Publisher, org.atlasapi.application.v3.SourceStatus> readsMap, Iterable<Publisher> order) {
        ImmutableList.Builder<SourceReadEntry> builder = ImmutableList.builder();
        for (Publisher source : order) {
            builder.add(new SourceReadEntry(
                      source,
                      SourceStatusModelTranslator.transform3To4(readsMap.get(source))
                    )
            );
        }   
        return builder.build();
    }
    

    
    public org.atlasapi.application.v3.Application transform4to3(Application input) {
        return org.atlasapi.application.v3.Application.application(input.getSlug())
                .withDeerId(input.getId().longValue())
                .withTitle(input.getTitle())
                .withDescription(input.getDescription())
                .createdAt(input.getCreated())
                .withConfiguration(transformSources4to3(input.getSources()))
                .withCredentials(transformCredentials4to3(input.getCredentials())) 
                .withRevoked(input.isRevoked())
                .build();
    }
    
    private org.atlasapi.application.v3.ApplicationCredentials transformCredentials4to3(ApplicationCredentials input) {
        return new org.atlasapi.application.v3.ApplicationCredentials(input.getApiKey());
    }
    
    private ApplicationConfiguration transformSources4to3(ApplicationSources input) {
        Map<Publisher, org.atlasapi.application.v3.SourceStatus> sourceStatuses = readsAsMap(input.getReads());
        ApplicationConfiguration configuration = ApplicationConfiguration.defaultConfiguration()
                .withSources(sourceStatuses);
        if (input.isPrecedenceEnabled()) {
            List<Publisher> precedence = Lists.transform(input.getReads(), SOURCEREADENTRY_TO_PUBLISHER);
            configuration = configuration.copyWithPrecedence(precedence);
        }
        configuration = configuration.copyWithWritableSources(input.getWrites());
        configuration = configuration.copyWithContentHierarchyPrecedence(input.contentHierarchyPrecedence().orNull());
        configuration = configuration.copyWithImagePrecedenceEnabled(input.imagePrecedenceEnabled());
        return configuration;
    }
    
    private Map<Publisher, org.atlasapi.application.v3.SourceStatus> readsAsMap(List<SourceReadEntry> input) {
        Map<Publisher, org.atlasapi.application.v3.SourceStatus> sourceStatuses = Maps.newHashMap();
        for (SourceReadEntry entry : input) {
            sourceStatuses.put(entry.getPublisher(), SourceStatusModelTranslator.transform4To3(entry.getSourceStatus()));
        }
        return sourceStatuses;
    }
}
