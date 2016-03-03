package org.atlasapi.query.v4.content;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.atlasapi.application.auth.InvalidApiKeyException;
import org.atlasapi.content.Content;
import org.atlasapi.content.ContentStore;
import org.atlasapi.content.QueryParseException;
import org.atlasapi.entity.Id;
import org.atlasapi.entity.util.Resolved;
import org.atlasapi.entity.util.WriteException;
import org.atlasapi.messaging.ResourceUpdatedMessage;
import org.atlasapi.query.common.QueryExecutionException;
import org.atlasapi.query.v4.content.deleteThis.ContentWritingWorker;
import org.atlasapi.query.v4.content.deleteThis.EntityUpdatedLegacyMessageSerializer;
import org.atlasapi.system.legacy.LegacyContentResolver;

import com.metabroadcast.common.ids.NumberToShortStringCodec;
import com.metabroadcast.common.properties.Configurer;
import com.metabroadcast.common.queue.MessageConsumerFactory;
import com.metabroadcast.common.queue.MessageSerializer;
import com.metabroadcast.common.queue.Worker;
import com.metabroadcast.common.queue.kafka.KafkaConsumer;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;

@Controller
@RequestMapping("/4/cql-content")
public class CqlContentShuffleController {

    private static final Logger log = LoggerFactory.getLogger(CqlContentShuffleController.class);

    private String contentChanges = Configurer.get("messaging.destination.content.changes").get();

    private final NumberToShortStringCodec idCodec;
    private final ContentStore yeOlde;
    private final ContentStore cql;
    private final KafkaConsumer consumer;

    public CqlContentShuffleController(
            NumberToShortStringCodec idCodec,
            ContentStore yeOlde,
            ContentStore cql,
            MessageConsumerFactory<KafkaConsumer> kafkaConsumerFactory,
            LegacyContentResolver legacyContentResolver) {
        this.idCodec = idCodec;
        this.yeOlde = yeOlde;
        this.cql = cql;

        MessageSerializer<ResourceUpdatedMessage> serializer =
                new EntityUpdatedLegacyMessageSerializer();

        Worker<ResourceUpdatedMessage> worker = new ContentWritingWorker(
                legacyContentResolver, cql
        );

        this.consumer = kafkaConsumerFactory.createConsumer(
                worker,
                serializer,
                contentChanges,
                "CqlContentStoreTestConsumer"
        ).build();
    }

    @RequestMapping("/ingest/start")
    public void ingestStart(
            HttpServletRequest request,
            HttpServletResponse response
    ) {
        log.info("Starting content ingester");
        consumer.startAsync().awaitRunning();
        log.info("Started content ingester");
    }

    @RequestMapping("/ingest/stop")
    public void ingestStop(
            HttpServletRequest request,
            HttpServletResponse response
    ) {
        log.info("Stopping content ingester");
        consumer.stopAsync().awaitTerminated();
        log.info("Stopped content ingester");
    }

    @RequestMapping("/migrate/{idString}")
    public void migrate(
            HttpServletRequest request,
            HttpServletResponse response,
            @PathVariable String idString
    ) throws ExecutionException, InterruptedException, WriteException {
        Id id = Id.valueOf(idCodec.decode(idString));
        Resolved<Content> content = yeOlde.resolveIds(ImmutableList.of(id)).get();
        cql.writeContent(Iterables.getOnlyElement(content.getResources()));
    }

    @RequestMapping("/resolve/{idString}")
    public void resolve(
            HttpServletRequest request,
            HttpServletResponse response,
            @PathVariable String idString
    ) throws ExecutionException, InterruptedException, IOException, QueryExecutionException,
            QueryParseException, InvalidApiKeyException {
        Id id = Id.valueOf(idCodec.decode(idString));
        Resolved<Content> content = cql.resolveIds(ImmutableList.of(id)).get();
        response.getWriter().write(content.getResources().toString());
    }
}
