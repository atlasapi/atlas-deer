package org.atlasapi.content.v2;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.atlasapi.content.Broadcast;
import org.atlasapi.content.ContainerRef;
import org.atlasapi.content.Content;
import org.atlasapi.content.ContentStore;
import org.atlasapi.content.ItemRef;
import org.atlasapi.content.SeriesRef;
import org.atlasapi.content.v2.serialization.ContentSerializationImpl;
import org.atlasapi.entity.Alias;
import org.atlasapi.entity.Id;
import org.atlasapi.entity.util.Resolved;
import org.atlasapi.entity.util.WriteException;
import org.atlasapi.entity.util.WriteResult;
import org.atlasapi.media.entity.Publisher;

import com.metabroadcast.common.collect.OptionalMap;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.mapping.Mapper;
import com.datastax.driver.mapping.MappingManager;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.joda.time.DateTime;

public class EmilsContentStore implements ContentStore {

    private final Session session;
    private final Mapper<org.atlasapi.content.v2.model.Content> mapper;
    private final ContentAccessor accessor;
    private final ContentSerializationImpl translator;

    public EmilsContentStore(Session session) {
        this.session = session;

        MappingManager mappingManager = new MappingManager(session);
        this.mapper = mappingManager
                .mapper(org.atlasapi.content.v2.model.Content.class);
        this.accessor = mappingManager.createAccessor(ContentAccessor.class);
        this.translator = new ContentSerializationImpl();
    }

    @Override
    public OptionalMap<Alias, Content> resolveAliases(Iterable<Alias> aliases, Publisher source) {
        throw new UnsupportedOperationException("herp derp");
    }

    @Override
    public WriteResult<Content, Content> writeContent(Content content)
            throws WriteException {
        BatchStatement batch = new BatchStatement();

        batch.addAll(StreamSupport.stream(translator.serialize(content).spliterator(), false)
                .map(serialised -> {
                    // TODO: this should not be a concern of the ContentStore impl I think
                    switch (serialised.getDiscriminator()) {
                        case org.atlasapi.content.v2.model.Content.ROW_MAIN:
                            return mapper.saveQuery(serialised);
                        case org.atlasapi.content.v2.model.Content.ROW_CLIPS:
                        case org.atlasapi.content.v2.model.Content.ROW_ENCODINGS:
                            return mapper.saveQuery(
                                    serialised,
                                    Mapper.Option.saveNullFields(false)
                            );
                        default:
                            throw new IllegalArgumentException(String.format(
                                    "Illegal row discriminator %s",
                                    serialised.getDiscriminator()
                            ));
                    }
                })
                .collect(Collectors.toList()));

        session.execute(batch);

        return new WriteResult<>(content, true, DateTime.now(), content);
    }

    @Override
    public void writeBroadcast(ItemRef item, Optional<ContainerRef> containerRef,
            Optional<SeriesRef> seriesRef, Broadcast broadcast) {
        throw new UnsupportedOperationException("herp derp");
    }

    @Override
    public ListenableFuture<Resolved<Content>> resolveIds(Iterable<Id> ids) {
        List<ListenableFuture<Content>> futures = StreamSupport.stream(ids.spliterator(), false)
                .map(Id::longValue)
                .map(accessor::getContent)
                .map(contentFuture -> Futures.transform(
                        contentFuture,
                        (ResultSet results) -> {
                            List<org.atlasapi.content.v2.model.Content> contents =
                                    mapper.map(results).all();
                            if (contents.isEmpty()) {
                                return null;
                            } else {
                                return translator.deserialize(contents);
                            }
                        }
                ))
                .collect(Collectors.toList());

        ListenableFuture<List<Content>> contentList = Futures.transform(
                Futures.allAsList(futures),
                (List<Content> input) -> input.stream()
                        .filter(Objects::nonNull)
                        .collect(Collectors.toList())
        );

        return Futures.transform(
                contentList,
                (Function<List<Content>, Resolved<Content>>) Resolved::valueOf
        );
    }
}
