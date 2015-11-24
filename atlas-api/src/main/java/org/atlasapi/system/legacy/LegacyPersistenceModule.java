package org.atlasapi.system.legacy;

import org.atlasapi.AtlasPersistenceModule;
import org.atlasapi.content.ContentResolver;
import org.atlasapi.content.NullContentResolver;
import org.atlasapi.event.EventResolver;
import org.atlasapi.media.segment.MongoSegmentResolver;
import org.atlasapi.messaging.v3.ScheduleUpdateMessage;
import org.atlasapi.organisation.OrganisationResolver;
import org.atlasapi.persistence.audit.NoLoggingPersistenceAuditLog;
import org.atlasapi.persistence.audit.PersistenceAuditLog;
import org.atlasapi.persistence.content.DefaultEquivalentContentResolver;
import org.atlasapi.persistence.content.EquivalentContentResolver;
import org.atlasapi.persistence.content.KnownTypeContentResolver;
import org.atlasapi.persistence.content.LookupResolvingContentResolver;
import org.atlasapi.persistence.content.mongo.MongoContentGroupResolver;
import org.atlasapi.persistence.content.mongo.MongoContentResolver;
import org.atlasapi.persistence.content.mongo.MongoTopicStore;
import org.atlasapi.persistence.content.organisation.MongoOrganisationStore;
import org.atlasapi.persistence.content.schedule.mongo.MongoScheduleStore;
import org.atlasapi.persistence.event.MongoEventStore;
import org.atlasapi.persistence.lookup.TransitiveLookupWriter;
import org.atlasapi.persistence.lookup.mongo.MongoLookupEntryStore;
import org.atlasapi.schedule.ScheduleResolver;
import org.atlasapi.topic.TopicResolver;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.metabroadcast.common.ids.SubstitutionTableNumberCodec;
import com.metabroadcast.common.persistence.mongo.DatabasedMongo;
import com.metabroadcast.common.queue.MessageSender;
import com.metabroadcast.common.queue.MessageSenders;
import com.mongodb.ReadPreference;

@Configuration
@Import(AtlasPersistenceModule.class)
public class LegacyPersistenceModule {

    @Autowired
    AtlasPersistenceModule persistence;

    @Bean
    @Qualifier("legacy")
    public ContentResolver legacyContentResolver() {
        DatabasedMongo mongoDb = persistence.databasedReadMongo();
        if (mongoDb == null) {
            return NullContentResolver.get();
        }
        KnownTypeContentResolver contentResolver = new MongoContentResolver(mongoDb, legacyEquivalenceStore());
        return new LegacyContentResolver(legacyEquivalenceStore(), contentResolver, legacySegmentMigrator(), persistence.channelStore());
    }

    @Bean
    @Qualifier("legacy")
    public ContentListerResourceListerAdapter legacyContentLister() {
        DatabasedMongo mongoDb = persistence.databasedReadMongo();
        LegacyMongoContentLister contentLister =
                new LegacyMongoContentLister(persistence.databasedReadMongo(), new MongoContentResolver(mongoDb, legacyEquivalenceStore()
        ));
        return new ContentListerResourceListerAdapter(
                contentLister,
                new LegacyContentTransformer(
                        persistence.channelStore(),
                        legacySegmentMigrator()
                )
        );
    }

    @Bean
    @Qualifier("legacy")
    public TopicResolver legacyTopicResolver() {
        return new LegacyTopicResolver(legacyTopicStore(), legacyTopicStore());
    }

    @Bean
    @Qualifier("legacy")
    public LegacyTopicLister legacyTopicLister() {
        return new LegacyTopicLister(legacyTopicStore());
    }

    @Bean
    @Qualifier("legacy")
    public MongoTopicStore legacyTopicStore() {
        return new MongoTopicStore(persistence.databasedReadMongo(), persistenceAuditLog());
    }

    @Bean
    @Qualifier("legacy")
    public MongoLookupEntryStore legacyEquivalenceStore() {
        return new MongoLookupEntryStore(
                persistence.databasedReadMongo().collection("lookup"),
                persistenceAuditLog(),
                ReadPreference.primaryPreferred());
    }

    @Bean
    @Qualifier("legacy")
    public ScheduleResolver legacyScheduleStore() {
        DatabasedMongo db = persistence.databasedReadMongo();
        KnownTypeContentResolver contentResolver = new MongoContentResolver(db, legacyEquivalenceStore());
        LookupResolvingContentResolver resolver = new LookupResolvingContentResolver(contentResolver, legacyEquivalenceStore());
        EquivalentContentResolver equivalentContentResolver = new DefaultEquivalentContentResolver(contentResolver, legacyEquivalenceStore());
        MessageSender<ScheduleUpdateMessage> sender = MessageSenders.noopSender();
        return new LegacyScheduleResolver(new MongoScheduleStore(db, persistence.channelStore(), resolver, equivalentContentResolver, sender), legacySegmentMigrator(), persistence.channelStore());
    }

    @Bean
    @Qualifier("legacy")
    public LegacySegmentMigrator legacySegmentMigrator() {
        return new LegacySegmentMigrator(legacySegmentResolver(), persistence.segmentStore());
    }

    @Bean
    @Qualifier("legacy")
    public org.atlasapi.media.segment.SegmentResolver legacySegmentResolver() {
        return new MongoSegmentResolver(persistence.databasedReadMongo(), new SubstitutionTableNumberCodec());
    }

    @Bean
    public LegacyContentGroupResolver legacyContentGroupResolver() {
        return new LegacyContentGroupResolver(new MongoContentGroupResolver(persistence.databasedReadMongo()));
    }

    @Bean
    @Qualifier("legacy")
    public EventResolver legacyEventResolver() {
        return new LegacyEventResolver(new MongoEventStore(persistence.databasedReadMongo()));
    }

    @Bean
    @Qualifier("legacy")
    public OrganisationResolver legacyOrganisationResolver() {
        TransitiveLookupWriter lookupWriter = TransitiveLookupWriter.generatedTransitiveLookupWriter(legacyEquivalenceStore());
        MongoOrganisationStore store = new MongoOrganisationStore(persistence.databasedReadMongo(),lookupWriter,legacyEquivalenceStore(),persistenceAuditLog());
        LegacyOrganisationTransformer transformer = new LegacyOrganisationTransformer();
        return new LegacyOrganisationResolver(store, transformer);
    }

    private PersistenceAuditLog persistenceAuditLog() {
        return new NoLoggingPersistenceAuditLog();
    }
}
