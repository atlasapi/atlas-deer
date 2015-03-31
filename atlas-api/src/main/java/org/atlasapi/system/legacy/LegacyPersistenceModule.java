package org.atlasapi.system.legacy;

import org.atlasapi.AtlasPersistenceModule;
import org.atlasapi.content.ContentResolver;
import org.atlasapi.content.NullContentResolver;
import org.atlasapi.media.entity.Described;
import org.atlasapi.media.segment.MongoSegmentResolver;
import org.atlasapi.messaging.v3.ScheduleUpdateMessage;
import org.atlasapi.persistence.audit.NoLoggingPersistenceAuditLog;
import org.atlasapi.persistence.audit.PersistenceAuditLog;
import org.atlasapi.persistence.content.DefaultEquivalentContentResolver;
import org.atlasapi.persistence.content.EquivalentContentResolver;
import org.atlasapi.persistence.content.KnownTypeContentResolver;
import org.atlasapi.persistence.content.LookupResolvingContentResolver;
import org.atlasapi.persistence.content.mongo.MongoContentLister;
import org.atlasapi.persistence.content.mongo.MongoContentResolver;
import org.atlasapi.persistence.content.mongo.MongoTopicStore;
import org.atlasapi.persistence.content.schedule.mongo.MongoScheduleStore;
import org.atlasapi.persistence.lookup.entry.LookupEntry;
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
    
    @Autowired AtlasPersistenceModule persistence;

    @Bean @Qualifier("legacy")
    public ContentResolver legacyContentResolver() {
        DatabasedMongo mongoDb = persistence.databasedMongo();
        if (mongoDb == null) {
            return NullContentResolver.get();
        }
        KnownTypeContentResolver contentResolver = new MongoContentResolver(mongoDb, legacyEquivalenceStore());
        return new LegacyContentResolver(legacyEquivalenceStore(), contentResolver, legacySegmentMigrator(), persistence.channelStore());
    }
    
    @Bean @Qualifier("legacy")
    public ContentListerResourceListerAdapter legacyContentLister() {
        DatabasedMongo mongoDb = persistence.databasedMongo();
        MongoContentLister contentLister = new MongoContentLister(
                persistence.databasedMongo(),
                new MongoContentResolver(mongoDb, legacyEquivalenceStore()
                )
        );
        return new ContentListerResourceListerAdapter(
                contentLister,
                new LegacyContentTransformer(
                        persistence.channelStore(),
                        legacySegmentMigrator()
                )
        );
    }
    
    @Bean @Qualifier("legacy")
    public TopicResolver legacyTopicResolver() {
        return new LegacyTopicResolver(legacyTopicStore(), legacyTopicStore());
    }

    @Bean @Qualifier("legacy")
    public LegacyTopicLister legacyTopicLister() {
        return new LegacyTopicLister(legacyTopicStore());
    }
    
    @Bean @Qualifier("legacy")
    public MongoTopicStore legacyTopicStore() {
        return new MongoTopicStore(persistence.databasedMongo(), persistenceAuditLog());
    }

    @Bean @Qualifier("legacy")
    public MongoLookupEntryStore legacyEquivalenceStore() {
        return new MongoLookupEntryStore(
                persistence.databasedMongo().collection("lookup"),
                persistenceAuditLog(),
                ReadPreference.primaryPreferred());
    }
    
    @Bean @Qualifier("legacy")
    public ScheduleResolver legacyScheduleStore() {
        DatabasedMongo db = persistence.databasedMongo();
        KnownTypeContentResolver contentResolver = new MongoContentResolver(db, legacyEquivalenceStore());
        LookupResolvingContentResolver resolver = new LookupResolvingContentResolver(contentResolver, legacyEquivalenceStore());
        EquivalentContentResolver equivalentContentResolver = new DefaultEquivalentContentResolver(contentResolver, legacyEquivalenceStore());
        MessageSender<ScheduleUpdateMessage> sender = MessageSenders.noopSender();
        return new LegacyScheduleResolver(new MongoScheduleStore(db, persistence.channelStore(), resolver, equivalentContentResolver, sender), legacySegmentMigrator(), persistence.channelStore());
    }

    @Bean @Qualifier("legacy")
    public LegacySegmentMigrator legacySegmentMigrator() {
        return new LegacySegmentMigrator(legacySegmentResolver(), persistence.segmentStore());
    }

    @Bean @Qualifier("legacy")
    public org.atlasapi.media.segment.SegmentResolver legacySegmentResolver() {
        return new MongoSegmentResolver(persistence.databasedMongo(), new SubstitutionTableNumberCodec());
    }

    private PersistenceAuditLog persistenceAuditLog() {
        return new NoLoggingPersistenceAuditLog();
    }
}
