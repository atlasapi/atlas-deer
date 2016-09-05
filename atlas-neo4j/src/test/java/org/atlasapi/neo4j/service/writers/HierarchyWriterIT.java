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

import com.codahale.metrics.Timer;
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
        contentWriter = ContentWriter.create(new Timer(), new Timer(), new Timer());
        hierarchyWriter = HierarchyWriter.create(contentWriter, new Timer()
        );
    }

    @Test
    public void writeBrandHierarchy() throws Exception {
        Brand brand = getBrand(0L);
        Series series = getSeries(1L);
        Episode episodeA = getEpisode(2L);
        Episode episodeB = getEpisode(3L);

        contentWriter.writeContentRef(brand.toRef(), session);

        brand.setSeriesRefs(ImmutableList.of(series.toRef()));
        brand.setItemRefs(ImmutableList.of(
                episodeA.toRef(),
                episodeB.toRef()
        ));

        hierarchyWriter.writeBrand(brand, session);

        checkChildren(brand, Series.class, ImmutableList.of(series));
        checkChildren(brand, Episode.class, ImmutableList.of(episodeA, episodeB));
    }

    @Test
    public void writeBrandWithNoHierarchy() throws Exception {
        Brand brand = getBrand(0L);

        contentWriter.writeContentRef(brand.toRef(), session);

        hierarchyWriter.writeBrand(brand, session);

        checkChildren(brand, Series.class, ImmutableList.of());
        checkChildren(brand, Episode.class, ImmutableList.of());
    }

    @Test
    public void removePartOfBrandHierarchy() throws Exception {
        Brand brand = getBrand(0L);
        Series series = getSeries(1L);
        Episode episodeA = getEpisode(2L);
        Episode episodeB = getEpisode(3L);

        contentWriter.writeContentRef(brand.toRef(), session);

        brand.setSeriesRefs(ImmutableList.of(series.toRef()));
        brand.setItemRefs(ImmutableList.of(
                episodeA.toRef(),
                episodeB.toRef()
        ));

        hierarchyWriter.writeBrand(brand, session);

        brand.setSeriesRefs(ImmutableList.of());
        brand.setItemRefs(ImmutableList.of(episodeA.toRef()));

        hierarchyWriter.writeBrand(brand, session);

        checkChildren(brand, Series.class, ImmutableList.of());
        checkChildren(brand, Episode.class, ImmutableList.of(episodeA));
    }

    @Test
    public void removeAllBrandHierarchy() throws Exception {
        Brand brand = getBrand(0L);
        Series series = getSeries(1L);
        Episode episodeA = getEpisode(2L);
        Episode episodeB = getEpisode(3L);

        contentWriter.writeContentRef(brand.toRef(), session);

        brand.setSeriesRefs(ImmutableList.of(series.toRef()));
        brand.setItemRefs(ImmutableList.of(
                episodeA.toRef(),
                episodeB.toRef()
        ));

        hierarchyWriter.writeBrand(brand, session);

        brand.setSeriesRefs(ImmutableList.of());
        brand.setItemRefs(ImmutableList.of());

        hierarchyWriter.writeBrand(brand, session);

        checkChildren(brand, Series.class, ImmutableList.of());
        checkChildren(brand, Episode.class, ImmutableList.of());
    }

    @Test
    public void updateBrandHierarchyUpdatesExistingParents() throws Exception {
        Brand brand = getBrand(0L);
        Series series = getSeries(1L);
        Episode episode = getEpisode(2L);

        Brand oldParent = getBrand(3L);

        contentWriter.writeContentRef(brand.toRef(), session);
        contentWriter.writeContentRef(series.toRef(), session);
        contentWriter.writeContentRef(episode.toRef(), session);

        series.setBrand(oldParent);
        hierarchyWriter.writeSeries(series, session);

        episode.setContainer(oldParent);
        hierarchyWriter.writeEpisode(episode, session);

        brand.setSeriesRefs(ImmutableList.of(series.toRef()));
        brand.setItemRefs(ImmutableList.of(episode.toRef()));

        hierarchyWriter.writeBrand(brand, session);

        checkParent(series, brand);
        checkParent(episode, brand);
    }

    @Test
    public void writeSeriesHierarchy() throws Exception {
        Brand brand = getBrand(0L);
        Series series = getSeries(1L);
        Episode episodeA = getEpisode(2L);
        Episode episodeB = getEpisode(3L);

        contentWriter.writeContentRef(series.toRef(), session);

        series.setBrand(brand);
        series.setItemRefs(ImmutableList.of(
                episodeA.toRef(),
                episodeB.toRef()
        ));

        hierarchyWriter.writeSeries(series, session);

        checkParent(series, brand);
        checkChildren(series, Episode.class, ImmutableList.of(episodeA, episodeB));
    }

    @Test
    public void writeSeriesWithNoHierarchy() throws Exception {
        Series series = getSeries(0L);

        contentWriter.writeContentRef(series.toRef(), session);

        series.setItemRefs(ImmutableList.of());

        hierarchyWriter.writeSeries(series, session);

        checkNoParent(series);
        checkChildren(series, Episode.class, ImmutableList.of());
    }

    @Test
    public void removePartOfSeriesHierarchy() throws Exception {
        Brand brand = getBrand(0L);
        Series series = getSeries(1L);
        Episode episodeA = getEpisode(2L);
        Episode episodeB = getEpisode(3L);

        contentWriter.writeContentRef(series.toRef(), session);

        series.setBrand(brand);
        series.setItemRefs(ImmutableList.of(
                episodeA.toRef(),
                episodeB.toRef()
        ));

        hierarchyWriter.writeSeries(series, session);

        series.setBrandRef(null);
        series.setItemRefs(ImmutableList.of(episodeA.toRef()));

        hierarchyWriter.writeSeries(series, session);

        checkNoParent(series);
        checkChildren(series, Episode.class, ImmutableList.of(episodeA));
    }

    @Test
    public void removeAllSeriesHierarchy() throws Exception {
        Brand brand = getBrand(0L);
        Series series = getSeries(1L);
        Episode episodeA = getEpisode(2L);
        Episode episodeB = getEpisode(3L);

        contentWriter.writeContentRef(series.toRef(), session);

        series.setBrand(brand);
        series.setItemRefs(ImmutableList.of(
                episodeA.toRef(),
                episodeB.toRef()
        ));

        hierarchyWriter.writeSeries(series, session);

        series.setBrandRef(null);
        series.setItemRefs(ImmutableList.of());

        hierarchyWriter.writeSeries(series, session);

        checkNoParent(series);
        checkChildren(series, Episode.class, ImmutableList.of());
    }

    @Test
    public void updateSeriesHierarchyUpdatesExistingParents() throws Exception {
        Brand brand = getBrand(0L);
        Series series = getSeries(1L);
        Episode episode = getEpisode(2L);

        Brand oldSeriesBrand = getBrand(3L);
        Series oldEpisodeSeries = getSeries(4L);

        contentWriter.writeContentRef(series.toRef(), session);
        contentWriter.writeContentRef(episode.toRef(), session);

        series.setBrand(oldSeriesBrand);
        hierarchyWriter.writeSeries(series, session);

        episode.setContainer(oldEpisodeSeries);
        hierarchyWriter.writeEpisode(episode, session);

        series.setBrand(brand);
        series.setItemRefs(ImmutableList.of(episode.toRef()));

        hierarchyWriter.writeSeries(series, session);

        checkParent(series, brand);
        checkParent(episode, series);
    }

    @Test
    public void writeEpisodeWithBrandParentHierarchy() throws Exception {
        Brand brand = getBrand(0L);
        Episode episode = getEpisode(2L);

        contentWriter.writeContentRef(episode.toRef(), session);

        episode.setContainer(brand);

        hierarchyWriter.writeEpisode(episode, session);

        checkParent(episode, brand);
    }

    @Test
    public void writeEpisodeWithSeriesParentHierarchy() throws Exception {
        Series series = getSeries(1L);
        Episode episode = getEpisode(2L);

        contentWriter.writeContentRef(episode.toRef(), session);

        episode.setContainer(series);

        hierarchyWriter.writeEpisode(episode, session);

        checkParent(episode, series);
    }

    @Test
    public void writeEpisodeWithNoHierarchy() throws Exception {
        Episode episode = getEpisode(2L);

        contentWriter.writeContentRef(episode.toRef(), session);

        hierarchyWriter.writeEpisode(episode, session);

        checkNoParent(episode);
    }

    @Test
    public void removeEpisodeHierarchy() throws Exception {
        Series series = getSeries(1L);
        Episode episode = getEpisode(2L);

        contentWriter.writeContentRef(episode.toRef(), session);

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

        contentWriter.writeContentRef(episode.toRef(), session);

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

        contentWriter.writeContentRef(episode.toRef(), session);

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

        contentWriter.writeContentRef(episode.toRef(), session);

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

        contentWriter.writeContentRef(episode.toRef(), session);

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

        contentWriter.writeContentRef(episode.toRef(), session);

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

        contentWriter.writeContentRef(episode.toRef(), session);

        episode.setContainer(brand);
        episode.setSeries(oldSeries);
        hierarchyWriter.writeEpisode(episode, session);

        episode.setSeriesRef(null);
        hierarchyWriter.writeEpisode(episode, session);

        checkParent(episode, brand);
        checkNoParent(episode, Series.class);
    }

    @Test
    public void writeNoHierarchy() throws Exception {
        Brand brand = getBrand(0L);
        Series series = getSeries(1L);
        Episode episode = getEpisode(2L);

        contentWriter.writeContentRef(series.toRef(), session);

        series.setBrand(brand);
        series.setItemRefs(ImmutableList.of(episode.toRef()));

        hierarchyWriter.writeSeries(series, session);

        hierarchyWriter.writeNoHierarchy(series, session);

        checkNoParent(series);
        checkChildren(series, Episode.class, ImmutableList.of());
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
