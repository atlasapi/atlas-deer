package org.atlasapi.neo4j.service.writers;

import org.atlasapi.content.Content;
import org.atlasapi.content.ContentType;
import org.atlasapi.content.Episode;
import org.atlasapi.content.Series;

import com.google.common.collect.ImmutableMap;
import org.neo4j.driver.v1.Statement;
import org.neo4j.driver.v1.StatementRunner;

import static org.atlasapi.neo4j.service.model.Neo4jContent.CONTENT;
import static org.atlasapi.neo4j.service.model.Neo4jContent.CONTENT_EPISODE_NUMBER;
import static org.atlasapi.neo4j.service.model.Neo4jContent.CONTENT_ID;
import static org.atlasapi.neo4j.service.model.Neo4jContent.CONTENT_SERIES_NUMBER;
import static org.atlasapi.neo4j.service.model.Neo4jContent.CONTENT_SOURCE;
import static org.atlasapi.neo4j.service.model.Neo4jContent.CONTENT_TYPE;

public class ContentWriter extends Neo4jWriter {

    private final Statement writeContentStatement;
    private final Statement writeSeriesStatement;
    private final Statement writeEpisodeStatement;

    private ContentWriter() {
        this.writeContentStatement = new Statement(""
                + "MERGE (content:" + CONTENT
                + " { " + CONTENT_ID + ": " + param(CONTENT_ID) + " }) "
                + "ON CREATE SET "
                + "content." + CONTENT_SOURCE + " = " + param(CONTENT_SOURCE) + " "
                + "SET "
                + "content." + CONTENT_TYPE + " = " + param(CONTENT_TYPE) + " "
                + "REMOVE "
                + "content." + CONTENT_EPISODE_NUMBER + ", "
                + "content." + CONTENT_SERIES_NUMBER);

        this.writeSeriesStatement = new Statement(""
                + "MERGE (content:" + CONTENT
                + " { " + CONTENT_ID + ": " + param(CONTENT_ID) + " }) "
                + "ON CREATE SET "
                + "content." + CONTENT_SOURCE + " = " + param(CONTENT_SOURCE) + " "
                + "SET "
                + "content." + CONTENT_TYPE + " = " + param(CONTENT_TYPE) + ", "
                + "content." + CONTENT_SERIES_NUMBER + " = " + param(CONTENT_SERIES_NUMBER));

        this.writeEpisodeStatement = new Statement(""
                + "MERGE (content:" + CONTENT
                + " { " + CONTENT_ID + ": " + param(CONTENT_ID) + " }) "
                + "ON CREATE SET "
                + "content." + CONTENT_SOURCE + " = " + param(CONTENT_SOURCE) + " "
                + "SET "
                + "content." + CONTENT_TYPE + " = " + param(CONTENT_TYPE) + ", "
                + "content." + CONTENT_EPISODE_NUMBER + " = " + param(CONTENT_EPISODE_NUMBER));
    }

    public static ContentWriter create() {
        return new ContentWriter();
    }

    public void writeSeries(Series series, StatementRunner runner) {
        ImmutableMap<String, Object> commonParameters = getCommonParameters(series);

        if (series.getSeriesNumber() != null) {
            write(
                    writeSeriesStatement.withParameters(ImmutableMap.<String, Object>builder()
                            .putAll(commonParameters)
                            .put(CONTENT_SERIES_NUMBER, series.getSeriesNumber())
                            .build()),
                    runner
            );
        } else {
            write(
                    writeContentStatement.withParameters(commonParameters),
                    runner
            );
        }
    }

    public void writeEpisode(Episode episode, StatementRunner runner) {
        ImmutableMap<String, Object> commonParameters = getCommonParameters(episode);

        if (episode.getEpisodeNumber() != null) {
            write(
                    writeEpisodeStatement.withParameters(
                            ImmutableMap.<String, Object>builder()
                                    .putAll(commonParameters)
                                    .put(CONTENT_EPISODE_NUMBER, episode.getEpisodeNumber())
                                    .build()
                    ),
                    runner
            );
        } else {
            write(
                    writeContentStatement.withParameters(commonParameters),
                    runner
            );
        }
    }

    public void writeContent(Content content, StatementRunner runner) {
        ImmutableMap<String, Object> commonParameters = getCommonParameters(content);
        write(
                writeContentStatement.withParameters(commonParameters),
                runner
        );
    }

    private ImmutableMap<String, Object> getCommonParameters(Content content) {
        return ImmutableMap.of(
                    CONTENT_ID, content.getId().longValue(),
                    CONTENT_SOURCE, content.getSource().key(),
                    CONTENT_TYPE, ContentType.fromContent(content).get().getKey()
            );
    }
}
