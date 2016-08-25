package org.atlasapi.content.v2;

import org.atlasapi.content.Broadcast;
import org.atlasapi.content.ContainerRef;
import org.atlasapi.content.Content;
import org.atlasapi.content.ContentWriter;
import org.atlasapi.content.Episode;
import org.atlasapi.content.ItemRef;
import org.atlasapi.content.SeriesRef;
import org.atlasapi.content.v2.serialization.ContentSerialization;
import org.atlasapi.content.v2.serialization.ContentSerializationImpl;
import org.atlasapi.entity.util.WriteException;
import org.atlasapi.entity.util.WriteResult;

import com.metabroadcast.common.time.Clock;

import com.datastax.driver.core.Session;
import com.datastax.driver.mapping.Mapper;
import com.datastax.driver.mapping.MappingManager;
import com.google.common.base.Optional;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class NonValidatingCqlWriter implements ContentWriter {

    private static final Logger log = LoggerFactory.getLogger(NonValidatingCqlWriter.class);

    private final Session session;
    private final Mapper<org.atlasapi.content.v2.model.Content> mapper;
    private final Clock clock;

    private final ContentSerialization translator = new ContentSerializationImpl();

    public static NonValidatingCqlWriter create(
            Session session,
            Clock clock
    ) {
        return new NonValidatingCqlWriter(session, clock);
    }

    private NonValidatingCqlWriter(
            Session session,
            Clock clock
    ) {
        this.session = checkNotNull(session);
        this.clock = checkNotNull(clock);

        MappingManager mappingManager = new MappingManager(session);
        this.mapper = mappingManager.mapper(org.atlasapi.content.v2.model.Content.class);
    }

    @Override
    public WriteResult<Content, Content> writeContent(Content content) throws WriteException {
        checkArgument(
                !(content instanceof Episode) || ((Episode) content).getContainerRef() != null,
                "Can't write episode without brand"
        );
        checkArgument(
                content.getId() != null,
                "Can't blindly write content without ID"
        );

        DateTime now = clock.now();

        org.atlasapi.content.v2.model.Content serialized = translator.serialize(content);
        log.debug("Force writing content {}", content.getId());
        session.execute(mapper.saveQuery(serialized));

        return new WriteResult<>(content, true, now, content);
    }

    @Override
    public void writeBroadcast(ItemRef item, Optional<ContainerRef> containerRef,
            Optional<SeriesRef> seriesRef, Broadcast broadcast) {
        throw new UnsupportedOperationException("NOPE");
    }
}
