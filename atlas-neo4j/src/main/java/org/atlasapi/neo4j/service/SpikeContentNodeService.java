package org.atlasapi.neo4j.service;

import java.util.stream.StreamSupport;

import org.atlasapi.content.Brand;
import org.atlasapi.content.ContainerRef;
import org.atlasapi.content.Content;
import org.atlasapi.content.ContentType;
import org.atlasapi.content.Episode;
import org.atlasapi.content.Location;
import org.atlasapi.content.Series;
import org.atlasapi.entity.Id;
import org.atlasapi.entity.ResourceRef;
import org.atlasapi.equivalence.EquivalenceGraph;
import org.atlasapi.util.ImmutableCollectors;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.joda.time.DateTime;
import org.neo4j.ogm.model.Result;
import org.neo4j.ogm.session.Session;

import static com.google.common.base.Preconditions.checkNotNull;

public class SpikeContentNodeService {

    private final Session session;

    private SpikeContentNodeService(Session session) {
        this.session = checkNotNull(session);
    }

    public static SpikeContentNodeService create(Session session) {
        return new SpikeContentNodeService(session);
    }

    public void writeEquivalentSet(EquivalenceGraph graph, Iterable<Content> contentInSet) {
        ImmutableSet<Id> writenContentIds = StreamSupport.stream(contentInSet.spliterator(), false)
                .filter(this::writeContent)
                .map(Content::getId)
                .collect(ImmutableCollectors.toSet());

        writeGraph(graph, writenContentIds);
    }

    private boolean writeContent(Content content) {
        if (content instanceof Brand) {
            Brand brand = (Brand) content;
            writeBrand(brand);

            brand.getSeriesRefs().stream()
                    .map(ResourceRef::getId)
                    .forEach(seriesId -> writeBrandToSeriesRelationship(brand.getId(), seriesId));

            brand.getItemRefs().stream()
                    .map(ResourceRef::getId)
                    .forEach(itemId -> writeBrandToEpisodeRelationship(brand.getId(), itemId));

            return true;
        }
        else if (content instanceof Series) {
            Series series = (Series) content;
            writeSeries(series);

            if (series.getBrandRef() != null) {
                writeBrandToSeriesRelationship(series.getBrandRef().getId(), series.getId());
            }

            series.getItemRefs().stream()
                    .map(ResourceRef::getId)
                    .forEach(itemId -> writeSeriesToEpisodeRelationship(series.getId(), itemId));

            return true;
        }
        else if (content instanceof Episode) {
            Episode episode = (Episode) content;
            writeEpisode(episode);

            if (episode.getManifestedAs() != null) {
                deleteExistingLocations(episode);

                ImmutableList<Location> locations = episode.getManifestedAs().stream()
                        .flatMap(manifestedAd -> manifestedAd.getAvailableAt().stream())
                        .collect(ImmutableCollectors.toList());

                ImmutableList<Long> locationIds = locations.stream()
                        .filter(location -> location.getPolicy() != null)
                        .map(this::writeLocation)
                        .collect(ImmutableCollectors.toList());

                StreamSupport.stream(locationIds.spliterator(), false)
                        .forEach(location -> addLocationToEpisode(episode, location));
            }

            ContainerRef containerRef = episode.getContainerRef();
            if (containerRef != null) {
                if (containerRef.getContentType() == ContentType.BRAND) {
                    writeBrandToEpisodeRelationship(containerRef.getId(), episode.getId());
                }
                else if (containerRef.getContentType() == ContentType.SERIES) {
                    writeSeriesToEpisodeRelationship(containerRef.getId(), episode.getId());
                }
            }

            return true;
        }
        return false;
    }

    private void writeBrand(Brand brand) {
        String query = "MERGE (brand:Brand { id: {id} })\n"
                + "ON CREATE SET brand.source = {source}\n"
                + "RETURN id(brand)";

        session.query(query, ImmutableMap.of(
                "id", brand.getId().longValue(),
                "source", brand.getSource().key()
        ));
    }

    private void writeSeries(Series series) {
        String query = "MERGE (series:Series { id: {id} })\n"
                + "ON CREATE SET series.source = {source}\n"
                + "RETURN id(series)";

        session.query(query, ImmutableMap.of(
                "id", series.getId().longValue(),
                "source", series.getSource().key()
        ));
    }

    private void writeEpisode(Episode episode) {
        String query = "MERGE (episode:Episode { id: {id} })\n"
                + "ON CREATE SET episode.source = {source}\n"
                + "RETURN id(episode)";

        session.query(query, ImmutableMap.of(
                "id", episode.getId().longValue(),
                "source", episode.getSource().key()
        ));
    }

    private void deleteExistingLocations(Episode episode) {
        String query =
                "MATCH (episode:Episode { id: {episodeId} })-[:HAS_LOCATION]->(location:Location)\n"
                        + "DETACH DELETE location";

        session.query(query, ImmutableMap.of(
                "episodeId", episode.getId().longValue()
        ));
    }

    private Long writeLocation(Location location) {
        String query = "CREATE (location:Location { start: {start}, end: {end} })\n"
                + "RETURN id(location) AS id";

        DateTime availabilityStart = location.getPolicy().getAvailabilityStart();
        DateTime availabilityEnd = location.getPolicy().getAvailabilityEnd();

        Result result = session.query(query, ImmutableMap.of(
                "start", availabilityStart != null ? availabilityStart.toString() : "",
                "end", availabilityEnd != null ? availabilityEnd.toString() : ""
        ));
        return Long.valueOf(result.iterator().next().get("id").toString());
    }

    private void addLocationToEpisode(Episode episode, Long locationId) {
        String query = "MATCH (episode:Episode { id: {episodeId} }), (location:Location)\n"
                + "WHERE id(location) = {locationId}\n"
                + "MERGE (episode)-[r:HAS_LOCATION]->(location)\n"
                + "RETURN id(r)";

        session.query(query, ImmutableMap.of(
                "episodeId", episode.getId().longValue(),
                "locationId", locationId
        ));
    }

    private void writeGraph(EquivalenceGraph graph, ImmutableSet<Id> writenContentIds) {
        graph.getAdjacencyList().entrySet().stream()
                .filter(entry -> writenContentIds.contains(entry.getKey()))
                .forEach(entry -> writeEquivalenceRelationships(
                        entry.getKey(), entry.getValue(), writenContentIds
                ));
    }

    private void writeEquivalenceRelationships(Id sourceNode, EquivalenceGraph.Adjacents adjacents,
            ImmutableSet<Id> writenContentIds) {
        adjacents.getEfferent().stream()
                .filter(resourceRef -> writenContentIds.contains(resourceRef.getId()))
                .forEach(resourceRef -> writeEquivalenceRelationship(
                        sourceNode, resourceRef.getId())
                );
    }

    private void writeEquivalenceRelationship(Id sourceNode, Id targetNode) {
        String query = "MATCH (source { id: {sourceId} }), (target {id: {targetId} })\n"
                + "MERGE (source)-[r:IS_EQUIVALENT]->(target)\n"
                + "RETURN id(r)";

        session.query(query, ImmutableMap.of(
                "sourceId", sourceNode.longValue(),
                "targetId", targetNode.longValue()
        ));
    }

    private void writeBrandToSeriesRelationship(Id brandId, Id seriesId) {
        String query = "MATCH (brand:Brand { id: {brandId} }), (series:Series { id: {seriesId} })\n"
                + "MERGE (brand)-[r:HAS_CHILD]->(series)\n"
                + "RETURN id(r)";

        session.query(query, ImmutableMap.of(
                "brandId", brandId.longValue(),
                "seriesId", seriesId.longValue()
        ));
    }

    private void writeBrandToEpisodeRelationship(Id brandId, Id episodeId) {
        String query = "MATCH (brand:Brand { id: {brandId} }), (episode:Episode { id: {episodeId} })\n"
                + "MERGE (brand)-[r:HAS_CHILD]->(episode)\n"
                + "RETURN id(r)";

        session.query(query, ImmutableMap.of(
                "brandId", brandId.longValue(),
                "episodeId", episodeId.longValue()
        ));
    }

    private void writeSeriesToEpisodeRelationship(Id seriesId, Id episodeId) {
        String query = "MATCH (series:Series { id: {seriesId} }), (episode:Episode { id: {episodeId} })\n"
                + "MERGE (series)-[r:HAS_CHILD]->(episode)\n"
                + "RETURN id(r)";

        session.query(query, ImmutableMap.of(
                "seriesId", seriesId.longValue(),
                "episodeId", episodeId.longValue()
        ));
    }
}
