package org.atlasapi.system.bootstrap;

import static com.google.common.base.Preconditions.checkNotNull;

import org.atlasapi.AtlasPersistenceModule;
import org.atlasapi.PersistenceModule;
import org.atlasapi.channel.Channel;
import org.atlasapi.content.ContentIndex;
import org.atlasapi.content.ContentStore;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.schedule.ScheduleResolver;
import org.atlasapi.schedule.ScheduleWriter;
import org.atlasapi.system.bootstrap.workers.DirectAndExplicitEquivalenceMigrator;
import org.joda.time.Interval;

import com.google.common.base.Optional;


/**
 * A schedule bootstrapper that also migrates schedule content, including its
 * equivalent set. This is generally not required in normal operations, but useful
 * to force consistency between Owl and Deer content in the schedule.
 *
 */
public class ScheduleBootstrapWithContentMigrationTaskFactory 
            implements SourceChannelIntervalFactory<ChannelIntervalScheduleBootstrapTask> {

    private final ScheduleResolver scheduleResolver;
    private final ScheduleWriter scheduleWriter;
    private final ContentStore contentStore;
    private ContentBootstrapListener contentAndEquivalentsBoostrapListener;

    public ScheduleBootstrapWithContentMigrationTaskFactory(ScheduleResolver scheduleResolver,
            ScheduleWriter scheduleWriter, ContentStore contentStore, ContentIndex contentIndex,
            DirectAndExplicitEquivalenceMigrator equivalenceMigrator, AtlasPersistenceModule persistence) {
        this.scheduleResolver = checkNotNull(scheduleResolver);
        this.scheduleWriter = checkNotNull(scheduleWriter);
        this.contentStore = checkNotNull(contentStore);
        this.contentAndEquivalentsBoostrapListener = ContentBootstrapListener.builder()
                .withContentWriter(contentStore)
                .withEquivalenceMigrator(equivalenceMigrator)
                .withEquivalentContentStore(persistence.nullMessageSendingEquivalentContentStore())
                .withContentIndex(contentIndex)
                .withMigrateEquivalents(persistence.nullMessageSendingEquivalenceGraphStore())
                .build();
    }

    @Override
    public ChannelIntervalScheduleBootstrapTask create(Publisher source, Channel channel,
            Interval interval) {
        return new ChannelIntervalScheduleBootstrapTask(
                scheduleResolver,
                scheduleWriter,
                contentStore,
                source,
                channel,
                interval,
                Optional.of(contentAndEquivalentsBoostrapListener));
    }
}
