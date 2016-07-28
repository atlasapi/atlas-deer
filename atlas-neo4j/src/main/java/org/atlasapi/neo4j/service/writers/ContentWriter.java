package org.atlasapi.neo4j.service.writers;

import org.atlasapi.content.Brand;
import org.atlasapi.content.Clip;
import org.atlasapi.content.Content;
import org.atlasapi.content.ContentRef;
import org.atlasapi.content.ContentType;
import org.atlasapi.content.ContentVisitor;
import org.atlasapi.content.Episode;
import org.atlasapi.content.Film;
import org.atlasapi.content.Item;
import org.atlasapi.content.Series;
import org.atlasapi.content.Song;
import org.atlasapi.entity.ResourceRef;

import com.google.common.collect.ImmutableMap;
import org.neo4j.driver.v1.Statement;
import org.neo4j.driver.v1.StatementRunner;

import static org.atlasapi.neo4j.service.model.Neo4jContent.CONTENT_EPISODE_NUMBER;
import static org.atlasapi.neo4j.service.model.Neo4jContent.CONTENT_ID;
import static org.atlasapi.neo4j.service.model.Neo4jContent.CONTENT_SERIES_NUMBER;
import static org.atlasapi.neo4j.service.model.Neo4jContent.CONTENT_SOURCE;
import static org.atlasapi.neo4j.service.model.Neo4jContent.CONTENT_TYPE;

public class ContentWriter extends Neo4jWriter {

    private final Statement writeResourceRefStatement;
    private final Statement writeContentRefStatement;
    private final Statement writeContentStatement;
    private final Statement writeSeriesStatement;
    private final Statement writeEpisodeStatement;

    private ContentWriter() {
        writeResourceRefStatement = new Statement(""
                + "MERGE (content:Content { " + CONTENT_ID + ": " + parameter(CONTENT_ID) + " }) "
                + "SET content." + CONTENT_SOURCE + " = " + parameter(CONTENT_SOURCE));

        writeContentRefStatement = new Statement(""
                + "MERGE (content:Content { " + CONTENT_ID + ": " + parameter(CONTENT_ID) + " }) "
                + "SET "
                + "content." + CONTENT_SOURCE + " = " + parameter(CONTENT_SOURCE) + ", "
                + "content." + CONTENT_TYPE + " = " + parameter(CONTENT_TYPE));

        writeContentStatement = new Statement(""
                + "MERGE (content:Content { " + CONTENT_ID + ": " + parameter(CONTENT_ID) + " }) "
                + "SET "
                + "content." + CONTENT_SOURCE + " = " + parameter(CONTENT_SOURCE) + ", "
                + "content." + CONTENT_TYPE + " = " + parameter(CONTENT_TYPE) + " "
                + "REMOVE "
                + "content." + CONTENT_EPISODE_NUMBER + ", "
                + "content." + CONTENT_SERIES_NUMBER);

        writeSeriesStatement = new Statement(""
                + "MERGE (content:Content { " + CONTENT_ID + ": " + parameter(CONTENT_ID) + " }) "
                + "SET "
                + "content." + CONTENT_SOURCE + " = " + parameter(CONTENT_SOURCE) + ", "
                + "content." + CONTENT_TYPE + " = " + parameter(CONTENT_TYPE) + ", "
                + "content." + CONTENT_SERIES_NUMBER + " = " + parameter(CONTENT_SERIES_NUMBER));

        writeEpisodeStatement = new Statement(""
                + "MERGE (content:Content { " + CONTENT_ID + ": " + parameter(CONTENT_ID) + " }) "
                + "SET "
                + "content." + CONTENT_SOURCE + " = " + parameter(CONTENT_SOURCE) + ", "
                + "content." + CONTENT_TYPE + " = " + parameter(CONTENT_TYPE) + ", "
                + "content." + CONTENT_EPISODE_NUMBER + " = " + parameter(CONTENT_EPISODE_NUMBER));
    }

    public static ContentWriter create() {
        return new ContentWriter();
    }

    public void writeResourceRef(ResourceRef resourceRef, StatementRunner runner) {
        ImmutableMap<String, Object> statementParameters = ImmutableMap.of(
                CONTENT_ID, resourceRef.getId().longValue(),
                CONTENT_SOURCE, resourceRef.getSource().key()
        );

        write(
                writeResourceRefStatement.withParameters(statementParameters),
                runner
        );
    }

    public void writeContentRef(ContentRef contentRef, StatementRunner runner) {
        ImmutableMap<String, Object> statementParameters = ImmutableMap.of(
                CONTENT_ID, contentRef.getId().longValue(),
                CONTENT_SOURCE, contentRef.getSource().key(),
                CONTENT_TYPE, contentRef.getContentType().getKey()
        );

        write(
                writeContentRefStatement.withParameters(statementParameters),
                runner
        );
    }

    // TODO Move visitor to the service
    public void writeContent(Content content, StatementRunner runner) {
        ImmutableMap<String, Object> statementParameters = ImmutableMap.of(
                CONTENT_ID, content.getId().longValue(),
                CONTENT_SOURCE, content.getSource().key(),
                CONTENT_TYPE, ContentType.fromContent(content).get().getKey()
        );

        content.accept(new ContentVisitor<Void>() {

            @Override
            public Void visit(Brand brand) {
                write(
                        writeContentStatement.withParameters(statementParameters),
                        runner
                );
                return null;
            }

            @Override
            public Void visit(Series series) {
                write(
                        writeSeriesStatement.withParameters(ImmutableMap.<String, Object>builder()
                                .putAll(statementParameters)
                                .put(CONTENT_SERIES_NUMBER, series.getSeriesNumber())
                                .build()),
                        runner
                );
                return null;
            }

            @Override
            public Void visit(Episode episode) {
                write(
                        writeEpisodeStatement.withParameters(
                                ImmutableMap.<String, Object>builder()
                                        .putAll(statementParameters)
                                        .put(CONTENT_EPISODE_NUMBER, episode.getEpisodeNumber())
                                        .build()
                        ),
                        runner
                );
                return null;
            }

            @Override
            public Void visit(Film film) {
                write(
                        writeContentStatement.withParameters(statementParameters),
                        runner
                );
                return null;
            }

            @Override
            public Void visit(Song song) {
                write(
                        writeContentStatement.withParameters(statementParameters),
                        runner
                );
                return null;
            }

            @Override
            public Void visit(Item item) {
                write(
                        writeContentStatement.withParameters(statementParameters),
                        runner
                );
                return null;
            }

            @Override
            public Void visit(Clip clip) {
                write(
                        writeContentStatement.withParameters(statementParameters),
                        runner
                );
                return null;
            }
        });
    }
}
