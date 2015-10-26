package org.atlasapi.system.bootstrap;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import java.time.Duration;
import java.time.Instant;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.atlasapi.content.Brand;
import org.atlasapi.content.Container;
import org.atlasapi.content.Content;
import org.atlasapi.content.ContentIndex;
import org.atlasapi.content.ContentResolver;
import org.atlasapi.content.ContentVisitorAdapter;
import org.atlasapi.content.ContentWriter;
import org.atlasapi.content.EquivalentContentStore;
import org.atlasapi.content.Item;
import org.atlasapi.content.ItemRef;
import org.atlasapi.content.SeriesRef;
import org.atlasapi.entity.Id;
import org.atlasapi.entity.util.WriteResult;
import org.atlasapi.equivalence.EquivalenceGraphUpdate;
import org.atlasapi.segment.SegmentEvent;
import org.atlasapi.system.bootstrap.workers.DirectAndExplicitEquivalenceMigrator;
import org.atlasapi.system.legacy.LegacyPersistenceModule;
import org.atlasapi.system.legacy.LegacySegmentMigrator;
import org.atlasapi.system.legacy.UnresolvedLegacySegmentException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.Futures;

public class ContentBootstrapListener extends ContentVisitorAdapter<
        ContentBootstrapListener.Result> {

    private static final Logger LOG = LoggerFactory.getLogger(ContentBootstrapListener.class);

    private final ContentWriter contentWriter;
    private final DirectAndExplicitEquivalenceMigrator equivalenceMigrator;
    private final EquivalentContentStore equivalentContentStore;
    private final ContentIndex contentIndex;

    private final boolean migrateHierarchy;
    private final LegacySegmentMigrator legacySegmentMigrator;
    private final ContentResolver legacyContentResolver;

    private ContentBootstrapListener(ContentWriter contentWriter,
            DirectAndExplicitEquivalenceMigrator equivalenceMigrator,
            EquivalentContentStore equivalentContentStore,
            ContentIndex contentIndex, boolean migrateHierarchy,
            LegacyPersistenceModule legacyPersistenceModule) {
        this.contentWriter = checkNotNull(contentWriter);
        this.equivalenceMigrator = checkNotNull(equivalenceMigrator);
        this.equivalentContentStore = checkNotNull(equivalentContentStore);
        this.contentIndex = checkNotNull(contentIndex);

        this.migrateHierarchy = checkNotNull(migrateHierarchy);
        checkArgument(!migrateHierarchy || legacyPersistenceModule != null);

        if (legacyPersistenceModule != null) {
            this.legacySegmentMigrator = legacyPersistenceModule.legacySegmentMigrator();
            this.legacyContentResolver = legacyPersistenceModule.legacyContentResolver();
        }
        else {
            this.legacySegmentMigrator = null;
            this.legacyContentResolver = null;
        }
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    protected Result visitItem(Item item) {
        ResultBuilder resultBuilder = resultBuilder();

        migrateContent(item, resultBuilder);

        if(migrateHierarchy && resultBuilder.isSucceeded()) {
            migrateSegments(item, resultBuilder);
        }

        return resultBuilder.build();
    }

    @Override
    protected Result visitContainer(Container container) {
        ResultBuilder resultBuilder = resultBuilder();

        migrateContent(container, resultBuilder);

        if(migrateHierarchy && resultBuilder.isSucceeded()) {
            migrateHierarchy(container, resultBuilder);
        }

        return resultBuilder.build();
    }

    private void migrateContent(Content content, ResultBuilder resultBuilder) {
        resultBuilder.addMessage("Migrating content " + content.getId().longValue());
        content.setReadHash(null);
        Instant start = Instant.now();

        try {
            WriteResult<Content, Content> writeResult = contentWriter.writeContent(content);
            Instant contentWriteEnd = Instant.now();
            if(!writeResult.written()) {
                resultBuilder.failure("No write occurred when migrating content into C* store");
                return;
            }
            resultBuilder.success("Migrated content into content store");

            Optional<EquivalenceGraphUpdate> graphUpdate =
                    equivalenceMigrator.migrateEquivalence(content.toRef());
            Instant graphUpdateEnd = Instant.now();
            resultBuilder.success("Migrated equivalence graph");

            equivalentContentStore.updateContent(content);

            if(graphUpdate.isPresent()) {
                equivalentContentStore.updateEquivalences(graphUpdate.get());
            }
            Instant equivalentContentUpdateEnd = Instant.now();
            resultBuilder.success("Migrated content into equivalent content store");

            contentIndex.index(content);
            Instant indexingEnd = Instant.now();
            resultBuilder.success("Indexed content");

            LOG.info(
                    "Update for {} write: {}ms, "
                            + "equiv graph update: {}ms, "
                            + "equiv content update: {}ms, "
                            + "index: {}ms, "
                            + "total: {}ms",
                    content.getId(),
                    Duration.between(start, contentWriteEnd).toMillis(),
                    Duration.between(contentWriteEnd, graphUpdateEnd).toMillis(),
                    Duration.between(graphUpdateEnd, equivalentContentUpdateEnd).toMillis(),
                    Duration.between(equivalentContentUpdateEnd, indexingEnd).toMillis(),
                    Duration.between(start, indexingEnd).toMillis()
            );
        } catch (Exception e) {
            LOG.error(String.format("Bootstrapping failure: %s %s", content.getId(), content), e);
            resultBuilder.failure("Failed to bootstrap content with error " + e.getMessage());
        }
    }

    private void migrateSegments(Item item, ResultBuilder resultBuilder) {
        Set<Id> successfullyMigrated = new HashSet<>();
        Set<Id> unsuccessfullyMigrated = new HashSet<>();

        Instant start = Instant.now();

        List<SegmentEvent> segmentEvents = item.getSegmentEvents();
        for(SegmentEvent segmentEvent : segmentEvents) {
            Id segmentId = segmentEvent.getSegmentRef().getId();
            if(migrateSegment(segmentId)) {
                successfullyMigrated.add(segmentId);
            }
            else {
                unsuccessfullyMigrated.add(segmentId);
            }
        }

        Instant end = Instant.now();

        addMigrationResult(resultBuilder, "segment events", successfullyMigrated,
                unsuccessfullyMigrated);

        if(unsuccessfullyMigrated.size() > 0) {
            LOG.warn("Failed to migrate all segment events for item {}: {}ms",
                    item.getId().longValue(), Duration.between(start, end).toMillis());
        }
        else {
            LOG.info("Migrated segment events for item {}: {}ms",
                    item.getId().longValue(), Duration.between(start, end).toMillis());
        }
    }

    private boolean migrateSegment(Id segmentId) {
        try {
            legacySegmentMigrator.migrateLegacySegment(segmentId);
            return true;
        } catch (UnresolvedLegacySegmentException e) {
            LOG.warn("Failed to migrate segment event {}", segmentId.longValue(), e);
            return false;
        }
    }

    private void migrateHierarchy(Container container, ResultBuilder resultBuilder) {
        Instant start = Instant.now();
        if(container instanceof Brand) {
            migrateSeries((Brand) container, resultBuilder);
        }

        migrateItemRefs(container, resultBuilder);
        Instant end = Instant.now();

        if(resultBuilder.isSucceeded()) {
            LOG.info("Migrated hierarchies for container {}: {}ms", container.getId().longValue(),
                    Duration.between(start, end).toMillis());
        }
        else {
            LOG.warn("Failed to migrate all hierarchies for container {}: {}ms",
                    container.getId().longValue(), Duration.between(start, end).toMillis());
        }
    }

    private void migrateSeries(Brand brand, ResultBuilder resultBuilder) {
        Set<Id> successfullyMigrated = new HashSet<>();
        Set<Id> unsuccessfullyMigrated = new HashSet<>();

        for (SeriesRef seriesRef : brand.getSeriesRefs()) {
            ResultBuilder seriesMigrationResultBuilder = resultBuilder();
            Id seriesRefId = seriesRef.getId();

            Content series = resolveLegacyContent(seriesRefId);
            migrateContent(series, seriesMigrationResultBuilder);

            if(seriesMigrationResultBuilder.isSucceeded() && series instanceof Container) {
                migrateHierarchy((Container) series, seriesMigrationResultBuilder);
            }

            if(seriesMigrationResultBuilder.isSucceeded()) {
                successfullyMigrated.add(seriesRefId);
            }
            else {
                unsuccessfullyMigrated.add(seriesRefId);
            }
        }

        addMigrationResult(resultBuilder, "series refs", successfullyMigrated,
                unsuccessfullyMigrated);
    }

    private void migrateItemRefs(Container container, ResultBuilder resultBuilder) {
        Set<Id> successfullyMigrated = new HashSet<>();
        Set<Id> unsuccessfullyMigrated = new HashSet<>();

        for (ItemRef itemRef : container.getItemRefs()) {
            ResultBuilder itemRefMigrationResultBuilder = resultBuilder();
            Id itemRefId = itemRef.getId();

            Content content = resolveLegacyContent(itemRefId);
            migrateContent(content, itemRefMigrationResultBuilder);

            if(itemRefMigrationResultBuilder.isSucceeded()) {
                successfullyMigrated.add(itemRefId);
            }
            else {
                unsuccessfullyMigrated.add(itemRefId);
            }
        }

        addMigrationResult(resultBuilder, "item refs", successfullyMigrated,
                unsuccessfullyMigrated);
    }

    private Content resolveLegacyContent(Id id) {
        return Iterables.getOnlyElement(
                Futures.getUnchecked(legacyContentResolver.resolveIds(ImmutableList.of(id)))
                        .getResources()
        );
    }

    private void addMigrationResult(ResultBuilder resultBuilder, String type,
            Set<Id> successfullyMigrated, Set<Id> unsuccessfullyMigrated) {
        if(unsuccessfullyMigrated.size() > 0) {
            resultBuilder.failure("Failed to migrate all " + type + ". " +
                    getMigrationResult(successfullyMigrated, unsuccessfullyMigrated));
        }
        else {
            resultBuilder.success("Successfully migrated " + type + ". " +
                    getMigrationResult(successfullyMigrated, unsuccessfullyMigrated));
        }
    }

    private String getMigrationResult(Set<Id> successfullyMigrated,
            Set<Id> unsuccessfullyMigrated) {
        String successMessage = "Successfully migrated: " +
                successfullyMigrated.stream()
                        .map(Id::toString)
                        .collect(Collectors.joining(","));

        if(unsuccessfullyMigrated.size() > 0) {
            return successMessage +
                    " Unsuccessfully migrated: " +
                    unsuccessfullyMigrated.stream()
                            .map(Id::toString)
                            .collect(Collectors.joining(","));
        }

        return successMessage;
    }

    public static class Builder {
        private ContentWriter contentWriter;
        private DirectAndExplicitEquivalenceMigrator equivalenceMigrator;
        private EquivalentContentStore equivalentContentStore;
        private ContentIndex contentIndex;

        private boolean migrateHierarchy = false;
        private LegacyPersistenceModule legacyPersistenceModule = null;

        private Builder() { }

        public Builder withContentWriter(ContentWriter contentWriter) {
            this.contentWriter = contentWriter;
            return this;
        }

        public Builder withEquivalenceMigrator(DirectAndExplicitEquivalenceMigrator
                equivalenceMigrator) {
            this.equivalenceMigrator = equivalenceMigrator;
            return this;
        }

        public Builder withEquivalentContentStore(EquivalentContentStore equivalentContentStore) {
            this.equivalentContentStore = equivalentContentStore;
            return this;
        }

        public Builder withContentIndex(ContentIndex contentIndex) {
            this.contentIndex = contentIndex;
            return this;
        }

        public Builder withMigrateHierarchies(LegacyPersistenceModule legacyPersistenceModule) {
            this.migrateHierarchy = true;
            this.legacyPersistenceModule = legacyPersistenceModule;
            return this;
        }

        public ContentBootstrapListener build() {
            return new ContentBootstrapListener(
                    contentWriter,
                    equivalenceMigrator,
                    equivalentContentStore,
                    contentIndex,
                    migrateHierarchy,
                    legacyPersistenceModule
            );
        }
    }

    private ResultBuilder resultBuilder() {
        return new ResultBuilder();
    }

    public static class Result {
        private final boolean succeeded;
        private final String message;

        public Result(boolean succeeded, String message) {
            this.succeeded = succeeded;
            this.message = message;
        }

        public boolean isSucceeded() {
            return succeeded;
        }

        public String getMessage() {
            return message;
        }
    }

    private static class ResultBuilder {
        private boolean succeeded = true;
        private StringBuilder message = new StringBuilder();

        public ResultBuilder success(String message) {
            addMessage(message);
            return this;
        }

        public ResultBuilder failure(String message) {
            this.succeeded = false;
            addMessage(message);
            return this;
        }

        public ResultBuilder addMessage(String message) {
            if(this.message.length() > 0) {
                this.message.append("\n");
            }
            this.message.append(message);
            return this;
        }

        public boolean isSucceeded() {
            return succeeded;
        }

        public Result build() {
            return new Result(succeeded, message.toString());
        }
    }
}
