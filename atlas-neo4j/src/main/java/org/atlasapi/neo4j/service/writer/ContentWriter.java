package org.atlasapi.neo4j.service.writer;

import java.util.Map;
import java.util.Set;

import org.atlasapi.content.Brand;
import org.atlasapi.content.Clip;
import org.atlasapi.content.Content;
import org.atlasapi.content.ContentVisitor;
import org.atlasapi.content.Episode;
import org.atlasapi.content.Film;
import org.atlasapi.content.Item;
import org.atlasapi.content.Series;
import org.atlasapi.content.Song;
import org.atlasapi.neo4j.model.nodes.ContentNodeType;
import org.atlasapi.util.ImmutableCollectors;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;
import org.neo4j.ogm.model.Result;
import org.neo4j.ogm.session.Session;

import static com.google.common.base.Preconditions.checkNotNull;

public class ContentWriter {

    private static final String CONTENT_ID = "id";
    private static final String CONTENT_SOURCE = "source";
    private static final String SERIES_NUMBER = "seriesNumber";
    private static final String EPISODE_NUMBER = "episodeNumber";

    private final Session session;
    private final HierarchyWriter hierarchyWriter;
    private final BroadcastWriter broadcastWriter;
    private final LocationWriter locationWriter;

    private ContentWriter(
            Session session,
            HierarchyWriter hierarchyWriter,
            BroadcastWriter broadcastWriter,
            LocationWriter locationWriter
    ) {
        this.session = checkNotNull(session);
        this.hierarchyWriter = checkNotNull(hierarchyWriter);
        this.broadcastWriter = checkNotNull(broadcastWriter);
        this.locationWriter = checkNotNull(locationWriter);
    }

    public static ContentWriter create(Session session) {
        return new ContentWriter(
                session,
                HierarchyWriter.create(session),
                BroadcastWriter.create(session),
                LocationWriter.create(session)
        );
    }

    public Long write(Content content) {
        return content.accept(new ContentVisitor<Long>() {

            @Override
            public Long visit(Episode episode) {
                return writeEpisode(episode);
            }

            @Override
            public Long visit(Film film) {
                return writeItem(film, ContentNodeType.FILM);
            }

            @Override
            public Long visit(Song song) {
                return writeItem(song, ContentNodeType.SONG);
            }

            @Override
            public Long visit(Item item) {
                return writeItem(item, ContentNodeType.ITEM);
            }

            @Override
            public Long visit(Clip clip) {
                return writeItem(clip, ContentNodeType.CLIP);
            }

            @Override
            public Long visit(Brand brand) {
                return writeBrand(brand);
            }

            @Override
            public Long visit(Series series) {
                return writeSeries(series);
            }
        });
    }

    private Long writeEpisode(Episode episode) {
        Long nodeId = write(ContentNodeType.EPISODE, ImmutableMap.of(
                CONTENT_ID, episode.getId().longValue(),
                CONTENT_SOURCE, episode.getSource().key(),
                EPISODE_NUMBER, episode.getEpisodeNumber() != null ? episode.getEpisodeNumber() : ""
        ));
        writeLocations(episode);
        writeBroadcasts(episode);
        hierarchyWriter.writeEpisodeHierarchy(episode);
        return nodeId;
    }

    private Long writeItem(Item item, ContentNodeType type) {
        Long nodeId = write(type, ImmutableMap.of(
                CONTENT_ID, item.getId().longValue(),
                CONTENT_SOURCE, item.getSource().key()
        ));
        writeLocations(item);
        writeBroadcasts(item);
        return nodeId;
    }

    private Long writeBrand(Brand brand) {
        Long nodeId = write(ContentNodeType.BRAND, ImmutableMap.of(
                CONTENT_ID, brand.getId().longValue(),
                CONTENT_SOURCE, brand.getSource().key()
        ));
        writeLocations(brand);
        hierarchyWriter.writeBrandHierarchy(brand);
        return nodeId;
    }

    private Long writeSeries(Series series) {
        Long nodeId = write(ContentNodeType.SERIES, ImmutableMap.of(
                CONTENT_ID, series.getId().longValue(),
                CONTENT_SOURCE, series.getSource().key(),
                SERIES_NUMBER, series.getSeriesNumber() != null ? series.getSeriesNumber() : ""
        ));
        writeLocations(series);
        hierarchyWriter.writeSeriesHierarchy(series);
        return nodeId;
    }

    private Long write(ContentNodeType type, Map<String, Object> params) {
        String query = getMergeClause(type) + "\n"
                + getSetClause(params.keySet()) + "\n"
                + "RETURN id(content) AS id";

        return execute(query, params);
    }

    private Long execute(String query, Map<String, Object> params) {
        Result result = session.query(query, params);

        if (!result.iterator().hasNext()) {
            throw new RuntimeException("Failed to execute query: " + query);
        }

        return Long.valueOf(result.iterator().next().get("id").toString());
    }

    private String getMergeClause(ContentNodeType type) {
        return "MERGE (content:" + type.getType() + " { id: {" + CONTENT_ID + "} })";
    }

    private String getSetClause(Set<String> params) {
        ImmutableMap<String, String> paramSetters = params.stream()
                .collect(ImmutableCollectors.toMap(
                        param -> "content." + param,
                        param -> "{" + param + "}"
                ));

        String setClause = Joiner.on(", ")
                .withKeyValueSeparator(" = ")
                .join(paramSetters);

        return "SET " + setClause;
    }

    private void writeLocations(Content content) {
        locationWriter.deleteExistingLocations(content);

        content.getManifestedAs().stream()
                .flatMap(manifestedAd -> manifestedAd.getAvailableAt().stream())
                .forEach(location -> locationWriter.write(location, content));
    }

    private void writeBroadcasts(Item item) {
        broadcastWriter.deleteExistingBroadcasts(item);

        item.getBroadcasts().stream()
                .forEach(broadcast -> broadcastWriter.write(broadcast, item));
    }
}
