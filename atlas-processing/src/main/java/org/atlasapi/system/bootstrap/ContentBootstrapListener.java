package org.atlasapi.system.bootstrap;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;

import javax.annotation.Nullable;

import org.atlasapi.content.Brand;
import org.atlasapi.content.Container;
import org.atlasapi.content.Content;
import org.atlasapi.content.ContentIndex;
import org.atlasapi.content.ContentResolver;
import org.atlasapi.content.ContentVisitorAdapter;
import org.atlasapi.content.ContentWriter;
import org.atlasapi.content.Episode;
import org.atlasapi.content.EquivalentContentStore;
import org.atlasapi.content.Film;
import org.atlasapi.content.Item;
import org.atlasapi.content.ItemRef;
import org.atlasapi.content.Series;
import org.atlasapi.content.SeriesRef;
import org.atlasapi.entity.Id;
import org.atlasapi.entity.util.WriteResult;
import org.atlasapi.equivalence.EquivalenceGraph;
import org.atlasapi.equivalence.EquivalenceGraphStore;
import org.atlasapi.equivalence.EquivalenceGraphUpdate;
import org.atlasapi.segment.SegmentEvent;
import org.atlasapi.system.bootstrap.workers.DirectAndExplicitEquivalenceMigrator;
import org.atlasapi.system.legacy.LegacySegmentMigrator;
import org.atlasapi.system.legacy.UnresolvedLegacySegmentException;

import com.metabroadcast.common.collect.OptionalMap;

import com.google.api.client.util.Lists;
import com.google.api.client.util.Maps;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.Futures;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class ContentBootstrapListener
        extends ContentVisitorAdapter<ContentBootstrapListener.Result> {

    private static final Logger log = LoggerFactory.getLogger(ContentBootstrapListener.class);

    private final ContentWriter contentWriter;
    private final DirectAndExplicitEquivalenceMigrator equivalenceMigrator;
    private final EquivalentContentStore equivalentContentStore;
    private final ContentIndex contentIndex;

    private final boolean migrateHierarchy;
    private final LegacySegmentMigrator legacySegmentMigrator;
    private final ContentResolver legacyContentResolver;

    private final boolean migrateEquivalents;
    private final EquivalenceGraphStore equivalenceGraphStore;

    private ContentBootstrapListener(
            ContentWriter contentWriter,
            DirectAndExplicitEquivalenceMigrator equivalenceMigrator,
            EquivalentContentStore equivalentContentStore,
            ContentIndex contentIndex,
            boolean migrateHierarchy,
            LegacySegmentMigrator legacySegmentMigrator,
            ContentResolver legacyContentResolver,
            boolean migrateEquivalents,
            @Nullable EquivalenceGraphStore equivalenceGraphStore
    ) {
        this.contentWriter = checkNotNull(contentWriter);
        this.equivalenceMigrator = checkNotNull(equivalenceMigrator);
        this.equivalentContentStore = checkNotNull(equivalentContentStore);
        this.contentIndex = checkNotNull(contentIndex);

        this.migrateHierarchy = migrateHierarchy;
        if (this.migrateHierarchy) {
            this.legacySegmentMigrator = checkNotNull(legacySegmentMigrator);
            this.legacyContentResolver = checkNotNull(legacyContentResolver);
        } else {
            this.legacySegmentMigrator = legacySegmentMigrator;
            this.legacyContentResolver = legacyContentResolver;
        }

        this.migrateEquivalents = checkNotNull(migrateEquivalents);
        checkArgument(!migrateEquivalents || equivalenceGraphStore != null);

        if (equivalenceGraphStore != null) {
            this.equivalenceGraphStore = checkNotNull(equivalenceGraphStore);
        } else {
            this.equivalenceGraphStore = null;
        }
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    protected Result visitItem(Item item) {
        Result result = Result.create();

        if (migrateHierarchy) {
            result.push(ResultNode.PARENTS);
            migrateParents(item, result);
            result.pop();
        }

        result.push(ResultNode.CONTENT);
        migrateContent(item, result);
        result.pop();

        if (migrateHierarchy && result.getSucceeded()) {
            result.push(ResultNode.SEGMENTS);
            migrateSegments(item, result);
            result.pop();
        }

        if (migrateEquivalents) {
            result.push(ResultNode.EQUIVALENTS);
            migrateEquivalents(item, result);
            result.pop();
        }

        logResult(item.getId(), result);

        return result;
    }

    @Override
    protected Result visitContainer(Container container) {
        Result result = Result.create();

        if (migrateHierarchy) {
            result.push(ResultNode.PARENTS);
            migrateParents(container, result);
            result.pop();
        }

        // These are both taken here because Someone thought making the model mutable was a great
        // idea. Tl; dr -- the Astyanax store mutates the content it writes and effectively resets
        // these refs to the ones stored in DB currently. Since the children migration further
        // down then uses the same, now mutated, object, we sometimes miss a load of children.
        // E.g., if the currently stored Brand only has 50 itemRefs, but the legacy resolved one
        // has 1042, by simply writing the brand, we'll reset its itemRefs to the 50 already stored,
        // and thus won't migrate over an additional 992 items. Rinse & repeat for seriesRefs.
        //
        // Yes, there **are** cases like this, how'd you think I found this?!
        ImmutableList<SeriesRef> seriesRefs = ImmutableList.of();
        if (container instanceof Brand) {
            seriesRefs = ((Brand) container).getSeriesRefs();
        }
        ImmutableList<ItemRef> itemRefs = container.getItemRefs();

        result.push(ResultNode.CONTENT);
        migrateContent(container, result);
        result.pop();

        if (migrateHierarchy && result.getSucceeded()) {
            result.push(ResultNode.CHILDREN);
            migrateHierarchy(
                    container,
                    result,
                    seriesRefs,
                    itemRefs
            );
            result.pop();
        }

        if (migrateEquivalents) {
            result.push(ResultNode.EQUIVALENTS);
            migrateEquivalents(container, result);
            result.pop();
        }

        logResult(container.getId(), result);

        return result;
    }

    private void migrateParents(Content content, Result result) {
        if (content instanceof Item) {
            migrateParentsForItem((Item) content, result);
        } else if (content instanceof Series) {
            migrateParentsForSeries((Series) content, result);
        }

    }

    private void migrateParentsForItem(Item item, Result result) {
        if (item instanceof Episode) {
            Episode episode = (Episode) item;
            if (episode.getSeriesRef() != null) {
                Id seriesId = episode.getSeriesRef().getId();
                migrateParent(seriesId, result);
            }
        }

        if (item.getContainerRef() != null) {
            Id containerId = item.getContainerRef().getId();
            migrateParent(containerId, result);
        }
    }

    private void migrateParentsForSeries(Series series, Result result) {
        if (series.getBrandRef() != null) {
            Id brandId = series.getBrandRef().getId();
            migrateParent(brandId, result);
        }
    }

    private void migrateParent(Id parentId, Result result) {
        try {
            Content content = resolveLegacyContent(parentId);
            migrateContent(content, result);
        } catch (Exception e) {
            log.error("Failed to migrate parent {}", parentId, e);
        }
    }

    private void migrateContent(Content content, Result result) {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("Migrating content ")
                .append(content.getId().longValue())
                .append(", ");

        content.setReadHash(null);

        try {
            WriteResult<Content, Content> writeResult = contentWriter.writeContent(content);
            if (!writeResult.written()) {
                result.failure("No write occurred when migrating content into C* store");
                return;
            }
            stringBuilder.append("Content store: DONE, ");

            Optional<EquivalenceGraphUpdate> graphUpdate =
                    equivalenceMigrator.migrateEquivalence(content.toRef());
            stringBuilder.append("Equivalence Graph: DONE, ");

            equivalentContentStore.updateContent(content.getId());

            if (graphUpdate.isPresent()) {
                equivalentContentStore.updateEquivalences(graphUpdate.get());
            }
            stringBuilder.append("Equivalent content store: DONE, ");

            contentIndex.index(content);

            stringBuilder.append("Index: DONE");

            result.success(stringBuilder.toString());
        } catch (Exception e) {
            log.error(String.format("Bootstrapping failure: %s %s", content.getId(), content), e);
            result.failure("Failed to bootstrap content with error " + e.getMessage());
        }
    }

    private void migrateSegments(Item item, Result result) {
        for (SegmentEvent segmentEvent : item.getSegmentEvents()) {
            Id segmentId = segmentEvent.getSegmentRef().getId();

            try {
                legacySegmentMigrator.migrateLegacySegment(segmentId);
                result.success("Migrated segment " + segmentId);
            } catch (UnresolvedLegacySegmentException e) {
                log.warn("Failed to migrate segment event {}", segmentId.longValue(), e);
                result.failure("Failed to migrate segment " + segmentId);
            }
        }
    }

    private void migrateHierarchy(
            Container container,
            Result result,
            ImmutableList<SeriesRef> seriesRefs,
            ImmutableList<ItemRef> itemRefs
    ) {
        if (container instanceof Brand) {
            migrateSeries(seriesRefs, result);
        }
        migrateItemRefs(itemRefs, result);
    }

    private void migrateSeries(ImmutableList<SeriesRef> seriesRefs, Result result) {
        for (SeriesRef seriesRef : seriesRefs) {
            try {
                Id seriesRefId = seriesRef.getId();

                Content series = resolveLegacyContent(seriesRefId);
                migrateContent(series, result);

                if (result.getSucceeded() && series instanceof Container) {
                    Container container = (Container) series;
                    migrateHierarchy(
                            container,
                            result,
                            ImmutableList.of(),
                            container.getItemRefs()
                    );
                }
            } catch (Exception e) {
                log.error("Failed to migrate series {}", seriesRef.getId(), e);
            }
        }
    }

    private void migrateItemRefs(ImmutableList<ItemRef> itemRefs, Result result) {
        for (ItemRef itemRef : itemRefs) {
            try {
                Id itemRefId = itemRef.getId();

                Content content = resolveLegacyContent(itemRefId);
                migrateContent(content, result);
            } catch (Exception e) {
                log.error("Failed to migrate item {}", itemRef.getId(), e);
            }
        }
    }

    private void migrateEquivalents(Content content, Result result) {
        OptionalMap<Id, EquivalenceGraph> equivalenceGraphOptional = Futures.getUnchecked(
                equivalenceGraphStore.resolveIds(ImmutableList.of(content.getId()))
        );

        if (equivalenceGraphOptional.get(content.getId()).isPresent()) {
            Set<Id> equivalentIds =
                    Sets.difference(
                            equivalenceGraphOptional.get(content.getId()).get()
                                    .getEquivalenceSet(),
                            ImmutableSet.of(content.getId())
                    );

            migrateEquivalents(equivalentIds, result);
        } else {
            String message = "Failed to find equivalence graph for " + content.getId().longValue();
            log.warn(message);
            result.failure(message);
        }
    }

    private void migrateEquivalents(Set<Id> equivalentIds, Result result) {
        for (Id equivalentId : equivalentIds) {
            try {
                Content content = resolveLegacyContent(equivalentId);

                // Some equivalents will fail to migrate because their parents are missing.
                // This is intended as a workaround for that scenario
                if (migrateHierarchy) {
                    result.push(ResultNode.PARENTS);
                    migrateParents(content, result);
                    result.pop();
                }

                migrateContent(content, result);
            } catch (Exception e) {
                log.error("Failed to migrate equivalent {}", equivalentId, e);
            }
        }
    }

    private Content resolveLegacyContent(Id id) {
        return Iterables.getOnlyElement(
                Futures.getUnchecked(
                        legacyContentResolver.resolveIds(
                                ImmutableList.of(id)
                        )
                ).getResources()
        );
    }

    private void logResult(Id id, Result result) {
        log.info(
                "Bootstrap of {} finished, success: {}, result: {}",
                id, result.getSucceeded(), result.toString().replaceAll("\n", " | ")
        );
    }

    public static class Builder {

        private ContentWriter contentWriter;
        private DirectAndExplicitEquivalenceMigrator equivalenceMigrator;
        private EquivalentContentStore equivalentContentStore;
        private ContentIndex contentIndex;

        private boolean migrateHierarchy = false;
        private LegacySegmentMigrator legacySegmentMigrator;
        private ContentResolver legacyContentResolver;

        private boolean migrateEquivalents = false;
        private EquivalenceGraphStore equivalenceGraphStore = null;

        private Builder() {
        }

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

        public Builder withSegmentMigratorAndContentResolver(
                LegacySegmentMigrator legacySegmentMigrator,
                ContentResolver legacyContentResolver) {
            this.legacySegmentMigrator = legacySegmentMigrator;
            this.legacyContentResolver = legacyContentResolver;
            return this;
        }

        public Builder withMigrateHierarchies(
                LegacySegmentMigrator legacySegmentMigrator,
                ContentResolver legacyContentResolver) {
            this.migrateHierarchy = true;
            return withSegmentMigratorAndContentResolver(
                    legacySegmentMigrator,
                    legacyContentResolver
            );
        }

        public Builder withMigrateEquivalents(EquivalenceGraphStore equivalenceGraphStore) {
            this.migrateEquivalents = true;
            this.equivalenceGraphStore = equivalenceGraphStore;
            return this;
        }

        public ContentBootstrapListener build() {
            return new ContentBootstrapListener(
                    contentWriter,
                    equivalenceMigrator,
                    equivalentContentStore,
                    contentIndex,
                    migrateHierarchy,
                    legacySegmentMigrator,
                    legacyContentResolver,
                    migrateEquivalents,
                    equivalenceGraphStore
            );
        }
    }

    /**
     * This class models the result of the bootstrap as an arbitrary graph. This is to allow
     * representing the bootstrap process which is fairly deeply nested.
     */
    public static class Result {

        private final ResultNode root = new ResultNode();

        private boolean succeeded = true;
        private Stack<String> currentPath = new Stack<>();

        private Result() { }

        public static Result create() {
            return new Result();
        }

        public ResultNode getRoot() {
            return root;
        }

        public boolean getSucceeded() {
            return succeeded;
        }

        public void push(String path) {
            checkArgument(ResultNode.ALLOWED_PATHS.contains(path));
            currentPath.push(path);
        }

        public void pop() {
            if (currentPath.isEmpty()) {
                return;
            }
            currentPath.pop();
        }

        public void success(String message) {
            ResultNode node = getCurrentNode();
            node.success(message);
        }

        public void failure(String message) {
            succeeded = false;
            ResultNode node = getCurrentNode();
            node.failure(message);
        }

        private ResultNode getCurrentNode() {
            ResultNode node = root;
            for (String path : currentPath) {
                node = node.getChild(path);
            }
            return node;
        }

        @Override
        public String toString() {
            StringBuilder stringBuilder = new StringBuilder();

            stringBuilder.append("Succeeded: ")
                    .append(getSucceeded())
                    .append("\n\n");

            appendMessages(stringBuilder, ResultNode.ROOT, root);

            return stringBuilder.toString();
        }

        private void appendMessages(StringBuilder builder, String nodeName, ResultNode node) {
            builder.append(nodeName)
                    .append(":\n");

            for (ResultMessage message : node.getMessages()) {
                builder.append(message.getMessage())
                        .append("\n");
            }

            builder.append("\n");

            for (Map.Entry<String, ResultNode> entry : node.getChildren().entrySet()) {
                appendMessages(builder, entry.getKey(), entry.getValue());
            }
        }
    }

    public static class ResultNode {

        public static final String ROOT = "Result";
        public static final String CONTENT = "Content";
        public static final String PARENTS = "Parents";
        public static final String CHILDREN = "Children";
        public static final String SEGMENTS = "Segments";
        public static final String EQUIVALENTS = "Equivalents";

        public static final List<String> ALLOWED_PATHS = ImmutableList.of(
                ROOT, CONTENT, PARENTS, CHILDREN, SEGMENTS, EQUIVALENTS
        );

        private final Map<String, ResultNode> children;
        private final List<ResultMessage> messages;

        public ResultNode() {
            this.children = Maps.newHashMap();
            this.messages = Lists.newArrayList();
        }

        public static ResultNode create() {
            return new ResultNode();
        }

        public Map<String, ResultNode> getChildren() {
            return children;
        }

        public List<ResultMessage> getMessages() {
            return messages;
        }

        public ResultNode getChild(String name) {
            checkArgument(ALLOWED_PATHS.contains(name));
            children.putIfAbsent(name, ResultNode.create());
            return children.get(name);
        }

        public void success(String message) {
            messages.add(ResultMessage.create(true, message));
        }

        public void failure(String message) {
            messages.add(ResultMessage.create(false, message));
        }
    }

    public static class ResultMessage {

        private final boolean success;
        private final String message;

        private ResultMessage(boolean success, String message) {
            this.success = success;
            this.message = checkNotNull(message);
        }

        public static ResultMessage create(boolean success, String message) {
            return new ResultMessage(success, message);
        }

        public boolean getSuccess() {
            return success;
        }

        public String getMessage() {
            return message;
        }
    }
}
