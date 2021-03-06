package org.atlasapi.neo4j.service.writers;

import java.util.List;

import org.atlasapi.content.Brand;
import org.atlasapi.content.Container;
import org.atlasapi.content.Content;
import org.atlasapi.content.ContentType;
import org.atlasapi.content.Episode;
import org.atlasapi.content.Series;
import org.atlasapi.entity.Id;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.neo4j.AbstractNeo4jIT;

import com.metabroadcast.common.stream.MoreCollectors;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.joda.time.DateTime;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.StatementResult;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

public class HierarchyWriterIT extends AbstractNeo4jIT {

    private ContentWriter contentWriter;
    private HierarchyWriter hierarchyWriter;

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        contentWriter = ContentWriter.create();
        hierarchyWriter = HierarchyWriter.create();
    }

    @Test
    public void writeBrandHierarchyDoesNotUpdateChildren() throws Exception {
        Brand brand = getBrand(0L);
        Series series = getSeries(1L);
        Episode episodeA = getEpisode(2L);
        Episode episodeB = getEpisode(3L);

        contentWriter.writeContent(brand, session);

        brand.setSeriesRefs(ImmutableList.of(series.toRef()));
        brand.setItemRefs(ImmutableList.of(
                episodeA.toRef(),
                episodeB.toRef()
        ));

        hierarchyWriter.writeBrand(brand, session);

        checkChildren(brand, Series.class, ImmutableList.of());
        checkChildren(brand, Episode.class, ImmutableList.of());
    }

    @Test
    public void writeBrandWithNoHierarchy() throws Exception {
        Brand brand = getBrand(0L);

        contentWriter.writeContent(brand, session);

        hierarchyWriter.writeBrand(brand, session);

        checkChildren(brand, Series.class, ImmutableList.of());
        checkChildren(brand, Episode.class, ImmutableList.of());
    }

    @Test
    public void writingBrandHierarchyRemovesExistingParent() throws Exception {
        Series contentWithOldType = getSeries(0L);
        Brand contentWithNewType = getBrand(0L);
        
        Brand oldParent = getBrand(1L);
        
        contentWriter.writeContent(contentWithOldType, session);
        
        contentWithOldType.setBrand(oldParent);
        hierarchyWriter.writeSeries(contentWithOldType, session);
        
        checkParent(contentWithOldType, oldParent);
        hierarchyWriter.writeBrand(contentWithNewType, session);
        
        checkNoParent(contentWithNewType);
    }

    @Test
    public void writingBrandWithNoSeriesOrEpisodeDoesNotRemoveThem() throws Exception {
        Brand brand = getBrand(0L);
        Series series = getSeries(1L);
        Episode episode = getEpisode(2L);
        
        contentWriter.writeContent(series, session);
        contentWriter.writeContent(episode, session);

        series.setBrand(brand);
        hierarchyWriter.writeSeries(series, session);
        
        episode.setContainer(brand);
        hierarchyWriter.writeEpisode(episode, session);
        
        hierarchyWriter.writeBrand(brand, session);
        
        checkChildren(brand, Series.class, ImmutableList.of(series));
        checkChildren(brand, Episode.class, ImmutableList.of(episode));
    }

    @Test
    public void writeSeriesHierarchyUpdatesParentButNotChildren() throws Exception {
        Brand brand = getBrand(0L);
        Series series = getSeries(1L);
        Episode episodeA = getEpisode(2L);
        Episode episodeB = getEpisode(3L);

        contentWriter.writeContent(series, session);

        series.setBrand(brand);
        series.setItemRefs(ImmutableList.of(
                episodeA.toRef(),
                episodeB.toRef()
        ));

        hierarchyWriter.writeSeries(series, session);

        checkParent(series, brand);
        checkChildren(series, Episode.class, ImmutableList.of());
    }

    @Test
    public void writeSeriesWithNoHierarchy() throws Exception {
        Series series = getSeries(0L);

        contentWriter.writeContent(series, session);

        series.setItemRefs(ImmutableList.of());
        hierarchyWriter.writeSeries(series, session);

        checkNoParent(series);
        checkChildren(series, Episode.class, ImmutableList.of());
    }

    @Test
    public void writingSeriesUpdatesParent() throws Exception {
        Brand oldParent = getBrand(0L);
        Brand newParent = getBrand(2L);
        Series series = getSeries(3L);
        
        contentWriter.writeContent(series, session);
        
        series.setBrand(oldParent);
        hierarchyWriter.writeSeries(series, session);

        series.setBrand(newParent);
        hierarchyWriter.writeSeries(series, session);

        checkParent(series, newParent);
    }

    @Test
    public void writingSeriesWithNoParentOrSubItemsRemovesParentButNotChildren() throws Exception {
        Brand parent = getBrand(0L);
        Series series = getSeries(1L);
        Episode episode = getEpisode(2L);
        
        contentWriter.writeContent(series, session);
        contentWriter.writeContent(episode, session);

        series.setBrand(parent);
        hierarchyWriter.writeSeries(series, session);
        
        episode.setContainer(series);
        hierarchyWriter.writeEpisode(episode, session);
        
        series.setBrandRef(null);
        hierarchyWriter.writeSeries(series, session);
        
        checkNoParent(series);
        checkChildren(series, Episode.class, ImmutableList.of(episode));
    }

    @Test
    public void writeEpisodeWithBrandParentHierarchy() throws Exception {
        Brand brand = getBrand(0L);
        Episode episode = getEpisode(2L);

        contentWriter.writeContent(episode, session);

        episode.setContainer(brand);

        hierarchyWriter.writeEpisode(episode, session);

        checkParent(episode, brand);
    }

    @Test
    public void writeEpisodeWithSeriesParentHierarchy() throws Exception {
        Series series = getSeries(1L);
        Episode episode = getEpisode(2L);

        contentWriter.writeContent(episode, session);

        episode.setContainer(series);

        hierarchyWriter.writeEpisode(episode, session);

        checkParent(episode, series);
    }

    @Test
    public void writeEpisodeWithNoHierarchy() throws Exception {
        Episode episode = getEpisode(2L);

        contentWriter.writeContent(episode, session);

        hierarchyWriter.writeEpisode(episode, session);

        checkNoParent(episode);
    }

    @Test
    public void removeEpisodeHierarchy() throws Exception {
        Series series = getSeries(1L);
        Episode episode = getEpisode(2L);

        contentWriter.writeContent(episode, session);

        episode.setContainer(series);

        hierarchyWriter.writeEpisode(episode, session);

        episode.setContainerRef(null);

        hierarchyWriter.writeEpisode(episode, session);

        checkNoParent(episode);
    }

    @Test
    public void updateEpisodeHierarchyRemovesPreviousParent() throws Exception {
        Series oldParent = getSeries(0L);
        Series newParent = getSeries(1L);
        Episode episode = getEpisode(2L);

        contentWriter.writeContent(episode, session);

        episode.setContainerRef(oldParent.toRef());
        hierarchyWriter.writeEpisode(episode, session);

        episode.setContainerRef(newParent.toRef());
        hierarchyWriter.writeEpisode(episode, session);

        checkParent(episode, newParent);
    }

    @Test
    public void writeEpisodeWithBothBrandAndSeriesParent() throws Exception {
        Brand brand = getBrand(0L);
        Series series = getSeries(1L);
        Episode episode = getEpisode(2L);

        contentWriter.writeContent(episode, session);

        episode.setContainer(brand);
        episode.setSeries(series);

        hierarchyWriter.writeEpisode(episode, session);

        checkParent(episode, brand);
        checkParent(episode, series);
    }

    @Test
    public void updateEpisodeParentBrandDoesNotAffectParentSeries() throws Exception {
        Brand oldBrand = getBrand(0L);
        Series series = getSeries(1L);
        Episode episode = getEpisode(2L);

        Brand newBrand = getBrand(3L);

        contentWriter.writeContent(episode, session);

        episode.setContainer(oldBrand);
        episode.setSeries(series);
        hierarchyWriter.writeEpisode(episode, session);

        episode.setContainer(newBrand);
        hierarchyWriter.writeEpisode(episode, session);

        checkParent(episode, newBrand);
        checkParent(episode, series);
    }

    @Test
    public void removeEpisodeParentBrandDoesNotAffectParentSeries() throws Exception {
        Brand oldBrand = getBrand(0L);
        Series series = getSeries(1L);
        Episode episode = getEpisode(2L);

        contentWriter.writeContent(episode, session);

        episode.setContainer(oldBrand);
        episode.setSeries(series);
        hierarchyWriter.writeEpisode(episode, session);

        episode.setContainerRef(null);
        hierarchyWriter.writeEpisode(episode, session);

        checkNoParent(episode, Brand.class);
        checkParent(episode, series);
    }

    @Test
    public void updateEpisodeParentSeriesDoesNotAffectParentBrand() throws Exception {
        Brand brand = getBrand(0L);
        Series oldSeries = getSeries(1L);
        Episode episode = getEpisode(2L);

        Series newSeries = getSeries(3L);

        contentWriter.writeContent(episode, session);

        episode.setContainer(brand);
        episode.setSeries(oldSeries);
        hierarchyWriter.writeEpisode(episode, session);

        episode.setSeries(newSeries);
        hierarchyWriter.writeEpisode(episode, session);

        checkParent(episode, brand);
        checkParent(episode, newSeries);
    }

    @Test
    public void removeEpisodeParentSeriesDoesNotAffectParentBrand() throws Exception {
        Brand brand = getBrand(0L);
        Series oldSeries = getSeries(1L);
        Episode episode = getEpisode(2L);

        contentWriter.writeContent(episode, session);

        episode.setContainer(brand);
        episode.setSeries(oldSeries);
        hierarchyWriter.writeEpisode(episode, session);

        episode.setSeriesRef(null);
        hierarchyWriter.writeEpisode(episode, session);

        checkParent(episode, brand);
        checkNoParent(episode, Series.class);
    }

    @Test
    public void writingEpisodeThatChangedTypeRemovesExistingChildren() throws Exception {
        Series contentWithOldType = getSeries(0L);
        Episode contentWithNewType = getEpisode(0L);

        Episode episode = getEpisode(1L);

        contentWriter.writeContent(episode, session);

        episode.setContainer(contentWithOldType);
        hierarchyWriter.writeEpisode(episode, session);

        checkParent(episode, contentWithOldType);

        hierarchyWriter.writeEpisode(contentWithNewType, session);

        checkNoParent(episode);
    }

    @Test
    public void writeNoHierarchy() throws Exception {
        Brand brand = getBrand(0L);
        Series series = getSeries(1L);
        Episode episode = getEpisode(2L);

        contentWriter.writeContent(series, session);

        series.setBrand(brand);
        hierarchyWriter.writeSeries(series, session);
        
        episode.setSeries(series);
        hierarchyWriter.writeEpisode(episode, session);

        hierarchyWriter.writeNoHierarchy(series, session);

        checkNoParent(series);
        checkChildren(series, Episode.class, ImmutableList.of());
    }

    @Test
    public void writingParentBrandWhenParentDoesNotExistWritesSourceAndType() throws Exception {
        Brand parent = getBrand(0L);
        Episode child = getEpisode(1L);

        contentWriter.writeContent(child, session);

        child.setContainer(parent);
        hierarchyWriter.writeEpisode(child, session);

        StatementResult result = session.run(
                "MATCH (parent:Content)<-[:HAS_BRAND]-(child:Content { id: {id} }) "
                        + "WHERE parent.type={type} AND parent.source={source}"
                        + "RETURN parent.id AS id",
                ImmutableMap.of(
                        "id", child.getId().longValue(),
                        "type", ContentType.fromContent(parent).get().getKey(),
                        "source", parent.getSource().key()
                )
        );

        assertThat(result.hasNext(), is(true));
    }

    @Test
    public void writingBrandParentUpdatesParentType() throws Exception {
        Series parentWithOldType = getSeries(0L);
        Brand parentWithNewType = getBrand(0L);

        Episode child = getEpisode(1L);

        contentWriter.writeContent(parentWithOldType, session);
        contentWriter.writeContent(child, session);

        child.setContainer(parentWithNewType);
        hierarchyWriter.writeEpisode(child, session);

        checkParent(child, parentWithNewType);
    }

    @Test
    public void writingParentSeriesWhenParentDoesNotExistWritesSourceAndType() throws Exception {
        Series parent = getSeries(0L);
        Episode child = getEpisode(1L);

        contentWriter.writeContent(child, session);

        child.setContainer(parent);
        hierarchyWriter.writeEpisode(child, session);

        StatementResult result = session.run(
                "MATCH (parent:Content)<-[:HAS_SERIES]-(child:Content { id: {id} }) "
                        + "WHERE parent.type={type} AND parent.source={source}"
                        + "RETURN parent.id AS id",
                ImmutableMap.of(
                        "id", child.getId().longValue(),
                        "type", ContentType.fromContent(parent).get().getKey(),
                        "source", parent.getSource().key()
                )
        );

        assertThat(result.hasNext(), is(true));
    }

    @Test
    public void writingSeriesParentUpdatesParentType() throws Exception {
        Brand parentWithOldType = getBrand(0L);
        Series parentWithNewType = getSeries(0L);

        Episode episode = getEpisode(1L);

        contentWriter.writeContent(parentWithOldType, session);
        contentWriter.writeContent(episode, session);

        episode.setContainer(parentWithNewType);
        hierarchyWriter.writeEpisode(episode, session);

        checkParent(episode, parentWithNewType);
    }

    private void checkChildren(Container parent, Class<? extends Content> childClass,
            List<Content> expectedChildren) {
        StatementResult result = session.run(
                "MATCH (parent:Content { id: {id} })<-[:HAS_BRAND|HAS_SERIES]-(child:Content) "
                        + "WHERE child.type={type} "
                        + "RETURN child.id AS id",
                ImmutableMap.of(
                        "id", parent.getId().longValue(),
                        "type", ContentType.fromContentClass().apply(childClass).get().getKey()
                )
        );

        ImmutableList<Long> actualChildIds = result.list()
                .stream()
                .map(record -> record.get("id").asLong())
                .collect(MoreCollectors.toImmutableList());

        ImmutableList<Long> expectedChildIds = expectedChildren.stream()
                .map(Content::getId)
                .map(Id::longValue)
                .collect(MoreCollectors.toImmutableList());

        assertThat(actualChildIds.size(), is(expectedChildren.size()));
        assertThat(actualChildIds.containsAll(expectedChildIds), is(true));
    }

    private void checkParent(Content child, Brand expectedParent) {
        StatementResult result = session.run(
                "MATCH (parent:Content)<-[:HAS_BRAND]-(child:Content { id: {id} }) "
                        + "WHERE parent.type={type} "
                        + "RETURN parent.id AS id",
                ImmutableMap.of(
                        "id", child.getId().longValue(),
                        "type", ContentType.fromContent(expectedParent).get().getKey()
                )
        );

        assertThat("No parent found", result.hasNext(), is(true));

        Record record = result.next();

        assertThat("Found child with multiple parents", result.hasNext(), is(false));

        assertThat(record.get("id").asLong(), is(expectedParent.getId().longValue()));
    }

    private void checkParent(Content child, Series expectedParent) {
        StatementResult result = session.run(
                "MATCH (parent:Content)<-[:HAS_SERIES]-(child:Content { id: {id} }) "
                        + "WHERE parent.type={type} "
                        + "RETURN parent.id AS id",
                ImmutableMap.of(
                        "id", child.getId().longValue(),
                        "type", ContentType.fromContent(expectedParent).get().getKey()
                )
        );

        assertThat("No parent found", result.hasNext(), is(true));

        Record record = result.next();

        assertThat("Found child with multiple parents", result.hasNext(), is(false));

        assertThat(record.get("id").asLong(), is(expectedParent.getId().longValue()));
    }

    private void checkNoParent(Content child, Class<? extends Container> parentType) {
        StatementResult result;

        if (parentType.equals(Brand.class)) {
            result = session.run(
                    "MATCH (parent:Content)<-[:HAS_BRAND]-(child:Content { id: {id} }) "
                            + "RETURN parent.id AS id",
                    ImmutableMap.of(
                            "id", child.getId().longValue()
                    )
            );
        } else {
            result = session.run(
                    "MATCH (parent:Content)<-[:HAS_SERIES]-(child:Content { id: {id} }) "
                            + "RETURN parent.id AS id",
                    ImmutableMap.of(
                            "id", child.getId().longValue()
                    )
            );
        }

        assertThat("Parent found, but expected none", result.hasNext(), is(false));
    }

    private void checkNoParent(Content child) {
        StatementResult result = session.run(
                "MATCH (parent:Content)<-[:HAS_BRAND|HAS_SERIES]-(child:Content { id: {id} }) "
                        + "RETURN parent.id AS id",
                ImmutableMap.of(
                        "id", child.getId().longValue()
                )
        );

        assertThat("Parent found, but expected none", result.hasNext(), is(false));
    }

    private Brand getBrand(long id) {
        Brand brand = new Brand(Id.valueOf(id), Publisher.METABROADCAST);
        brand.setThisOrChildLastUpdated(DateTime.now());

        return brand;
    }

    private Series getSeries(long id) {
        Series series = new Series(Id.valueOf(id), Publisher.METABROADCAST);
        series.setThisOrChildLastUpdated(DateTime.now());

        return series;
    }

    private Episode getEpisode(long id) {
        Episode episode = new Episode(Id.valueOf(id), Publisher.METABROADCAST);
        episode.setThisOrChildLastUpdated(DateTime.now());

        return episode;
    }
}
