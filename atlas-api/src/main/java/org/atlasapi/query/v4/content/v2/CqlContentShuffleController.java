package org.atlasapi.query.v4.content.v2;

import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import java.util.stream.StreamSupport;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.atlasapi.application.auth.InvalidApiKeyException;
import org.atlasapi.content.Content;
import org.atlasapi.content.ContentStore;
import org.atlasapi.content.QueryParseException;
import org.atlasapi.content.v2.EmilsContentStore;
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

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
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

    private final ExecutorService executor;
    private final NumberToShortStringCodec idCodec;
    private final ContentStore yeOlde;
    private final EmilsContentStore cql;
    private final KafkaConsumer consumer;

    private HistoryIngest historyIngester;
    private Future<Long> historyIngesterFuture;

    public CqlContentShuffleController(
            NumberToShortStringCodec idCodec,
            ContentStore yeOlde,
            EmilsContentStore cql,
            MessageConsumerFactory<KafkaConsumer> kafkaConsumerFactory,
            LegacyContentResolver legacyContentResolver
    ) {
        this.executor = Executors.newFixedThreadPool(1);
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

    @RequestMapping("/history/start/{startId}/{step}")
    public void backPopStart(
            HttpServletRequest request,
            HttpServletResponse response,
            @PathVariable Long startId,
            @PathVariable Long step
    ) {
        this.historyIngester = new HistoryIngest(startId, step);
        this.historyIngesterFuture = executor.submit(historyIngester);
    }

    @RequestMapping("/history/stop")
    public void backPopStop(
            HttpServletRequest request,
            HttpServletResponse response
    ) throws IOException, InterruptedException, ExecutionException, TimeoutException {
        if (historyIngester != null) {
            historyIngester.stop();
            response.getWriter().write(historyIngesterFuture.get(30, TimeUnit.SECONDS).toString());
        }
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

    private class HistoryIngest implements Callable<Long> {

        private final Long startId;
        private final Long step;
        private final AtomicBoolean keepGoing;

        public HistoryIngest(Long startId, Long step) {
            this.startId = startId;
            this.step = step;
            this.keepGoing = new AtomicBoolean(true);
        }

        @Override
        public Long call() throws Exception {
            Long currStart = startId;

            while (keepGoing.get()) {
                log.debug("Resolving content IDs {} - {}", currStart, currStart + step);
                ListenableFuture<Resolved<Content>> oldContent = yeOlde
                        .resolveIds(LongStream
                                .range(currStart, currStart + step)
                                .mapToObj(Id::valueOf)
                                .collect(Collectors.toList()));

                final Long finalCurrStart = currStart;
                ListenableFuture<Boolean> write = Futures.transform(
                        oldContent,
                        (Function<Resolved<Content>, Boolean>) input -> {
                            StreamSupport.stream(input.getResources().spliterator(), false)
                                    .forEach(content -> {
                                        try {
                                            cql.writeContent(content);
                                        } catch (Exception e) {
                                            log.error(
                                                    "Error writing content {}",
                                                    content.getId(),
                                                    e
                                            );
                                            throw new RuntimeException(String.format(
                                                    "Error writing content %s",
                                                    content.getId()
                                            ));
                                        }
                                    });

                            log.info(
                                    "Ingested {} contents in ID range {} - {}",
                                    input.getResources().size(),
                                    finalCurrStart,
                                    finalCurrStart + step
                            );

                            return !input.getResources().isEmpty();
                        }
                );

                write.get(30, TimeUnit.SECONDS);
                currStart += step;
            }

            return currStart;
        }

        public void stop() {
            this.keepGoing.set(false);
        }
    }
}
