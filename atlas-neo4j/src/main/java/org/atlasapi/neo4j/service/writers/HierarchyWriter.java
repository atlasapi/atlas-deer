package org.atlasapi.neo4j.service.writers;

import org.atlasapi.content.Brand;
import org.atlasapi.content.BrandRef;
import org.atlasapi.content.ContainerRef;
import org.atlasapi.content.Content;
import org.atlasapi.content.ContentRef;
import org.atlasapi.content.Episode;
import org.atlasapi.content.EpisodeRef;
import org.atlasapi.content.Series;
import org.atlasapi.content.SeriesRef;

import com.google.common.collect.ImmutableMap;
import org.neo4j.driver.v1.Statement;
import org.neo4j.driver.v1.StatementRunner;

import static org.atlasapi.neo4j.service.model.Neo4jContent.CONTENT;
import static org.atlasapi.neo4j.service.model.Neo4jContent.CONTENT_ID;
import static org.atlasapi.neo4j.service.model.Neo4jContent.CONTENT_SOURCE;
import static org.atlasapi.neo4j.service.model.Neo4jContent.CONTENT_TYPE;
import static org.atlasapi.neo4j.service.model.Neo4jContent.HAS_BRAND_RELATIONSHIP;
import static org.atlasapi.neo4j.service.model.Neo4jContent.HAS_SERIES_RELATIONSHIP;

/**
 * This class creates/updates the hierarchy relationships between brands, series and episodes.
 * Relationships are only updated "upwards", i.e. from episode to series to brand, but not in the
 * opposite direction. This is for performance reasons because some large flat brands can have
 * thousands of episodes and we don't want to have to update all those relationships every time
 */
public class HierarchyWriter extends Neo4jWriter {

    private static final String PARENT_ID_PARAM = "parentId";
    private static final String CHILD_ID_PARAM = "childId";
    private static final String PARENT_SOURCE_PARAM = "parentSource";
    private static final String PARENT_TYPE_PARAM = "parentType";

    private final Statement addBrandParentStatement;
    private final Statement addSeriesParentStatement;
    private final Statement removeParentStatement;
    private final Statement removeChildrenStatement;

    private HierarchyWriter() {
        this.addBrandParentStatement = new Statement(""
                + "MATCH "
                + "(child:"+ CONTENT + " { " + CONTENT_ID + ": " + param(CHILD_ID_PARAM) + " }) "
                + "OPTIONAL MATCH (existingParent:"+ CONTENT
                + ")<-[r:"+ HAS_BRAND_RELATIONSHIP + "]-(child) "
                + "DELETE r "
                + "MERGE "
                + "(parent:"+ CONTENT + " { " + CONTENT_ID + ": " + param(PARENT_ID_PARAM) + " }) "
                + "ON CREATE SET "
                + "parent." + CONTENT_SOURCE + " = " + param(PARENT_SOURCE_PARAM) + " "
                + "SET "
                + "parent." + CONTENT_TYPE + " = " + param(PARENT_TYPE_PARAM) + " "
                + "MERGE (parent)<-[:"+ HAS_BRAND_RELATIONSHIP + "]-(child)");

        this.addSeriesParentStatement = new Statement(""
                + "MATCH "
                + "(child:"+ CONTENT + " { " + CONTENT_ID + ": " + param(CHILD_ID_PARAM) + " }) "
                + "OPTIONAL MATCH (existingParent:"+ CONTENT
                + ")<-[r:"+ HAS_SERIES_RELATIONSHIP + "]-(child) "
                + "DELETE r "
                + "MERGE "
                + "(parent:"+ CONTENT + " { " + CONTENT_ID + ": " + param(PARENT_ID_PARAM) + " }) "
                + "ON CREATE SET "
                + "parent." + CONTENT_SOURCE + " = " + param(PARENT_SOURCE_PARAM) + " "
                + "SET "
                + "parent." + CONTENT_TYPE + " = " + param(PARENT_TYPE_PARAM) + " "
                + "MERGE (parent)<-[:"+ HAS_SERIES_RELATIONSHIP + "]-(child)");

        this.removeParentStatement = new Statement(""
                + "MATCH "
                + "(parent:"+ CONTENT + ")"
                + "<-[r:"+ HAS_BRAND_RELATIONSHIP + "|" + HAS_SERIES_RELATIONSHIP + "]-"
                + "(child:"+ CONTENT + " { " + CONTENT_ID + ": " + param(CHILD_ID_PARAM) + " }) "
                + "DELETE r");

        this.removeChildrenStatement = new Statement(""
                + "MATCH "
                + "(parent:"+ CONTENT + " { " + CONTENT_ID + ": " + param(PARENT_ID_PARAM) + " })"
                + "<-[r:"+ HAS_BRAND_RELATIONSHIP + "|" + HAS_SERIES_RELATIONSHIP + "]-"
                + "(child:"+ CONTENT + ") "
                + "DELETE r");
    }

    public static HierarchyWriter create() {
        return new HierarchyWriter();
    }

    /**
     * This will remove all existing hierarchy for a content.
     *
     * Since content can change type it's possible that some content may have the wrong hierarchy
     * for its type, e.g. if content X used to be a series under brand Y and then X changed type to
     * a brand its link to brand Y should be removed.
     */
    public void writeNoHierarchy(Content content, StatementRunner runner) {
        ContentRef contentRef = content.toRef();

        removeParent(contentRef, runner);
        removeChildren(contentRef, runner);
    }

    public void writeBrand(Brand brand, StatementRunner runner) {
        // If this brand used to have a different type (e.g. series) it may have an
        // existing parent. This removes it since brands can't have a parent
        removeParent(brand.toRef(), runner);
    }

    public void writeSeries(Series series, StatementRunner runner) {
        removeParent(series.toRef(), runner);

        if (series.getBrandRef() != null) {
            addParent(series.toRef(), series.getBrandRef(), runner);
        }
    }

    public void writeEpisode(Episode episode, StatementRunner runner) {
        // If this episode used to have a different type (e.g. series) it may have had
        // existing children. This removes them since episodes can't have children.
        removeChildren(episode.toRef(), runner);

        removeParent(episode.toRef(), runner);

        writeEpisodeHierarchy(episode, runner);
    }

    private void writeEpisodeHierarchy(Episode episode, StatementRunner runner) {
        EpisodeRef episodeRef = episode.toRef();

        ContainerRef containerRef = episode.getContainerRef();
        if (containerRef != null) {
            addParent(episodeRef, containerRef, runner);
        }

        SeriesRef seriesRef = episode.getSeriesRef();
        if (seriesRef != null) {
            addParent(episodeRef, seriesRef, runner);
        }
    }

    private void addParent(ContentRef child, ContainerRef parent, StatementRunner runner) {
        if (parent instanceof BrandRef) {
            addBrandParent(child, (BrandRef) parent, runner);
        } else if (parent instanceof SeriesRef) {
            addSeriesParent(child, (SeriesRef) parent, runner);
        } else {
            throw new IllegalArgumentException("Unknown containerRef type " + parent);
        }
    }

    private void addBrandParent(ContentRef child, BrandRef parent, StatementRunner runner) {
        write(
                addBrandParentStatement.withParameters(ImmutableMap.of(
                        PARENT_ID_PARAM, parent.getId().longValue(),
                        CHILD_ID_PARAM, child.getId().longValue(),
                        PARENT_SOURCE_PARAM, parent.getSource().key(),
                        PARENT_TYPE_PARAM, parent.getContentType().getKey()
                )),
                runner
        );
    }

    private void addSeriesParent(ContentRef child, SeriesRef parent, StatementRunner runner) {
        write(
                addSeriesParentStatement.withParameters(ImmutableMap.of(
                        PARENT_ID_PARAM, parent.getId().longValue(),
                        CHILD_ID_PARAM, child.getId().longValue(),
                        PARENT_SOURCE_PARAM, parent.getSource().key(),
                        PARENT_TYPE_PARAM, parent.getContentType().getKey()
                )),
                runner
        );
    }

    private void removeParent(ContentRef child, StatementRunner runner) {
        write(
                removeParentStatement.withParameters(ImmutableMap.of(
                        CHILD_ID_PARAM, child.getId().longValue()
                )),
                runner
        );
    }

    private void removeChildren(ContentRef parent, StatementRunner runner) {
        write(
                removeChildrenStatement.withParameters(ImmutableMap.of(
                        PARENT_ID_PARAM, parent.getId().longValue()
                )),
                runner
        );
    }
}
