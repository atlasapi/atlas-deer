package org.atlasapi.neo4j.service.writer;

import org.atlasapi.content.Brand;
import org.atlasapi.content.ContainerRef;
import org.atlasapi.content.ContentType;
import org.atlasapi.content.Episode;
import org.atlasapi.content.Series;
import org.atlasapi.content.SeriesRef;
import org.atlasapi.entity.Id;
import org.atlasapi.entity.ResourceRef;

import com.google.common.collect.ImmutableMap;
import org.neo4j.ogm.session.Session;

import static com.google.common.base.Preconditions.checkNotNull;

public class HierarchyWriter {

    private final Session session;

    private HierarchyWriter(Session session) {
        this.session = checkNotNull(session);
    }

    public static HierarchyWriter create(Session session) {
        return new HierarchyWriter(session);
    }

    public void writeBrandHierarchy(Brand brand) {
        brand.getSeriesRefs().stream()
                .map(ResourceRef::getId)
                .forEach(seriesId -> writeBrandToSeriesRelationship(brand.getId(), seriesId));

        brand.getItemRefs().stream()
                .map(ResourceRef::getId)
                .forEach(itemId -> writeBrandToEpisodeRelationship(brand.getId(), itemId));
    }

    public void writeSeriesHierarchy(Series series) {
        if (series.getBrandRef() != null) {
            writeBrandToSeriesRelationship(series.getBrandRef().getId(), series.getId());
        }

        series.getItemRefs().stream()
                .map(ResourceRef::getId)
                .forEach(itemId -> writeSeriesToEpisodeRelationship(series.getId(), itemId));

    }

    public void writeEpisodeHierarchy(Episode episode) {
        ContainerRef containerRef = episode.getContainerRef();
        if (containerRef != null) {
            if (containerRef.getContentType() == ContentType.BRAND) {
                writeBrandToEpisodeRelationship(containerRef.getId(), episode.getId());
            }
            else if (containerRef.getContentType() == ContentType.SERIES) {
                writeSeriesToEpisodeRelationship(containerRef.getId(), episode.getId());
            }
        }

        SeriesRef seriesRef = episode.getSeriesRef();
        if (seriesRef != null) {
            writeSeriesToEpisodeRelationship(seriesRef.getId(), episode.getId());
        }
    }

    private void writeBrandToSeriesRelationship(Id brandId, Id seriesId) {
        String query =
                "MATCH (brand:Brand { id: {brandId} }), (series:Series { id: {seriesId} })\n"
                        + "MERGE (brand)-[r:HAS_CHILD]->(series)\n"
                        + "RETURN id(r)";

        session.query(query, ImmutableMap.of(
                "brandId", brandId.longValue(),
                "seriesId", seriesId.longValue()
        ));
    }

    private void writeBrandToEpisodeRelationship(Id brandId, Id episodeId) {
        String query =
                "MATCH (brand:Brand { id: {brandId} }), (episode:Episode { id: {episodeId} })\n"
                        + "MERGE (brand)-[r:HAS_CHILD]->(episode)\n"
                        + "RETURN id(r)";

        session.query(query, ImmutableMap.of(
                "brandId", brandId.longValue(),
                "episodeId", episodeId.longValue()
        ));
    }

    private void writeSeriesToEpisodeRelationship(Id seriesId, Id episodeId) {
        String query =
                "MATCH (series:Series { id: {seriesId} }), (episode:Episode { id: {episodeId} })\n"
                        + "MERGE (series)-[r:HAS_CHILD]->(episode)\n"
                        + "RETURN id(r)";

        session.query(query, ImmutableMap.of(
                "seriesId", seriesId.longValue(),
                "episodeId", episodeId.longValue()
        ));
    }
}
