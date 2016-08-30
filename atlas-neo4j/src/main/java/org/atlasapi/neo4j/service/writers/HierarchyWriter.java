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

import static com.google.common.base.Preconditions.checkNotNull;
import static org.atlasapi.neo4j.service.model.Neo4jContent.CONTENT;
import static org.atlasapi.neo4j.service.model.Neo4jContent.CONTENT_ID;
import static org.atlasapi.neo4j.service.model.Neo4jContent.HAS_BRAND_RELATIONSHIP;
import static org.atlasapi.neo4j.service.model.Neo4jContent.HAS_SERIES_RELATIONSHIP;

public class HierarchyWriter extends Neo4jWriter {

    private static final String PARENT_ID_PARAM = "parentId";
    private static final String CHILD_ID_PARAM = "childId";

    private final ContentWriter contentWriter;

    private final Statement addBrandParentStatement;
    private final Statement addSeriesParentStatement;
    private final Statement removeParentStatement;
    private final Statement removeChildrenStatement;

    private HierarchyWriter(ContentWriter contentWriter) {
        this.contentWriter = checkNotNull(contentWriter);

        this.addBrandParentStatement = new Statement(""
                + "MATCH "
                + "(parent:"+ CONTENT + " { " + CONTENT_ID + ": " + param(PARENT_ID_PARAM) + " }), "
                + "(child:"+ CONTENT + " { " + CONTENT_ID + ": " + param(CHILD_ID_PARAM) + " }) "
                + "OPTIONAL MATCH (existingParent:"+ CONTENT
                + ")<-[r:"+ HAS_BRAND_RELATIONSHIP + "]-(child) "
                + "DELETE r "
                + "MERGE (parent)<-[:"+ HAS_BRAND_RELATIONSHIP + "]-(child)");

        this.addSeriesParentStatement = new Statement(""
                + "MATCH "
                + "(parent:"+ CONTENT + " { " + CONTENT_ID + ": " + param(PARENT_ID_PARAM) + " }), "
                + "(child:"+ CONTENT + " { " + CONTENT_ID + ": " + param(CHILD_ID_PARAM) + " }) "
                + "OPTIONAL MATCH (existingParent:"+ CONTENT
                + ")<-[r:"+ HAS_SERIES_RELATIONSHIP + "]-(child) "
                + "DELETE r "
                + "MERGE (parent)<-[:"+ HAS_SERIES_RELATIONSHIP + "]-(child)");

        this.removeParentStatement = new Statement(""
                + "MATCH "
                + "(parent:"+ CONTENT + ")"
                + "<-[r:"+ HAS_BRAND_RELATIONSHIP + "|HAS_SERIES]-"
                + "(child:"+ CONTENT + " { " + CONTENT_ID + ": " + param(CHILD_ID_PARAM) + " }) "
                + "DELETE r");

        this.removeChildrenStatement = new Statement(""
                + "MATCH "
                + "(parent:"+ CONTENT + " { " + CONTENT_ID + ": " + param(PARENT_ID_PARAM) + " })"
                + "<-[r:"+ HAS_BRAND_RELATIONSHIP + "|HAS_SERIES]-"
                + "(child:"+ CONTENT + ") "
                + "DELETE r");
    }

    public static HierarchyWriter create(ContentWriter contentWriter) {
        return new HierarchyWriter(contentWriter);
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
        writeNoHierarchy(brand, runner);
        writeBrandHierarchy(brand, runner);
    }

    public void writeSeries(Series series, StatementRunner runner) {
        writeNoHierarchy(series, runner);
        writeSeriesHierarchy(series, runner);
    }

    public void writeEpisode(Episode episode, StatementRunner runner) {
        writeNoHierarchy(episode, runner);
        writeEpisodeHierarchy(episode, runner);
    }

    private void writeBrandHierarchy(Brand brand, StatementRunner runner) {
        brand.getSeriesRefs()
                .forEach(seriesRef -> contentWriter.writeContentRef(seriesRef, runner));

        brand.getItemRefs()
                .forEach(itemRef -> contentWriter.writeContentRef(itemRef, runner));

        BrandRef brandRef = brand.toRef();

        removeChildren(brandRef, runner);

        brand.getSeriesRefs()
                .forEach(seriesRef -> addParent(seriesRef, brandRef, runner));

        brand.getItemRefs()
                .forEach(itemRef -> addParent(itemRef, brandRef, runner));
    }

    private void writeSeriesHierarchy(Series series, StatementRunner runner) {
        SeriesRef seriesRef = series.toRef();

        if (series.getBrandRef() != null) {
            contentWriter.writeContentRef(series.getBrandRef(), runner);
            addParent(seriesRef, series.getBrandRef(), runner);
        }

        removeChildren(seriesRef, runner);

        series.getItemRefs()
                .forEach(itemRef -> contentWriter.writeContentRef(itemRef, runner));

        series.getItemRefs()
                .forEach(itemRef -> addParent(itemRef, seriesRef, runner));
    }

    private void writeEpisodeHierarchy(Episode episode, StatementRunner runner) {
        EpisodeRef episodeRef = episode.toRef();

        ContainerRef containerRef = episode.getContainerRef();
        if (containerRef != null) {
            contentWriter.writeContentRef(containerRef, runner);
            addParent(episodeRef, containerRef, runner);
        }

        SeriesRef seriesRef = episode.getSeriesRef();
        if (seriesRef != null) {
            contentWriter.writeContentRef(seriesRef, runner);
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
                        CHILD_ID_PARAM, child.getId().longValue()
                )),
                runner
        );
    }

    private void addSeriesParent(ContentRef child, SeriesRef parent, StatementRunner runner) {
        write(
                addSeriesParentStatement.withParameters(ImmutableMap.of(
                        PARENT_ID_PARAM, parent.getId().longValue(),
                        CHILD_ID_PARAM, child.getId().longValue()
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
