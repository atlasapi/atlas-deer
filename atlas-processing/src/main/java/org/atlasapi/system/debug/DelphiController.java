package org.atlasapi.system.debug;

import com.google.common.collect.ImmutableList;
import com.google.common.net.HttpHeaders;
import com.google.gson.FieldNamingPolicy;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.metabroadcast.applications.client.model.internal.Application;
import com.metabroadcast.common.collect.OptionalMap;
import com.metabroadcast.common.ids.NumberToShortStringCodec;
import com.metabroadcast.common.ids.SubstitutionTableNumberCodec;
import com.metabroadcast.common.stream.MoreCollectors;
import com.metabroadcast.common.time.DateTimeZones;
import com.metabroadcast.common.webapp.serializers.JodaDateTimeSerializer;
import joptsimple.internal.Strings;
import org.atlasapi.application.ApplicationFetcher;
import org.atlasapi.application.ApplicationResolutionException;
import org.atlasapi.entity.Id;
import org.atlasapi.entity.ResourceRef;
import org.atlasapi.equivalence.EquivalenceGraph;
import org.atlasapi.equivalence.EquivalenceGraphStore;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.system.debug.serializers.AdjacentsSerializer;
import org.atlasapi.system.debug.serializers.EquivalenceGraphSerializer;
import org.atlasapi.system.debug.serializers.ResourceRefSerializer;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;

@Controller
public class DelphiController {

    private static final Logger log = LoggerFactory.getLogger(DelphiController.class);

    private final EquivalenceGraphStore equivalenceGraphStore;
    private final ApplicationFetcher applicationFetcher;
    private final NumberToShortStringCodec codec;
    private final Gson gson;

    private DelphiController(EquivalenceGraphStore equivalenceGraphStore, ApplicationFetcher applicationFetcher) {
        this.equivalenceGraphStore = equivalenceGraphStore;
        this.applicationFetcher = applicationFetcher;
        this.codec = SubstitutionTableNumberCodec.lowerCaseOnly();
        this.gson = new GsonBuilder()
                .setFieldNamingPolicy(FieldNamingPolicy.LOWER_CASE_WITH_UNDERSCORES)
                .registerTypeAdapter(DateTime.class, new JodaDateTimeSerializer())
                .registerTypeAdapter(EquivalenceGraph.class, EquivalenceGraphSerializer.create())
                .registerTypeAdapter(EquivalenceGraph.Adjacents.class, AdjacentsSerializer.create())
                .registerTypeAdapter(ResourceRef.class, ResourceRefSerializer.create())
                .create();
    }

    public static DelphiController create(
            EquivalenceGraphStore equivalenceGraphStore,
            ApplicationFetcher applicationFetcher
    ) {
        return new DelphiController(equivalenceGraphStore, applicationFetcher);
    }

    @RequestMapping(value = "/system/debug/delphi/graph/{id}.json", method = RequestMethod.GET)
    public void listEquivalenceGraph(
            HttpServletRequest request,
            HttpServletResponse response,
            @PathVariable("id") String id,
            @RequestParam(value = "key", required = false) String apiKey
    ) throws IOException {
        try {
            Id decodedId = Id.valueOf(codec.decode(id));

            OptionalMap<Id, EquivalenceGraph> optionalGraphMap = equivalenceGraphStore.resolveIds(
                    ImmutableList.of(decodedId)
            )
                    .get();

            if (optionalGraphMap.isEmpty()) {
                response.sendError(HttpStatus.NOT_FOUND.value());
                return;
            }

            EquivalenceGraph equivalenceGraph = optionalGraphMap.get(decodedId).get();

            if(!Strings.isNullOrEmpty(apiKey)) {
                equivalenceGraph = filterSources(equivalenceGraph, decodedId, apiKey);
            }

            response.addHeader(
                    HttpHeaders.ACCESS_CONTROL_ALLOW_ORIGIN,
                    "*"
            );
            gson.toJson(equivalenceGraph, response.getWriter());
        } catch (Exception e) {
            log.error("Request exception {}", request.getRequestURI(), e);
            response.sendError(HttpStatus.INTERNAL_SERVER_ERROR.value(), e.getMessage());
        }
    }

    private EquivalenceGraph filterSources(EquivalenceGraph equivalenceGraph, Id start, String apiKey)
            throws ApplicationResolutionException {
        Optional<Application> applicationOpt = applicationFetcher.applicationForApiKey(apiKey);
        if (!applicationOpt.isPresent()) {
            throw new IllegalArgumentException("No application found for apiKey " + apiKey);
        }

        Application application = applicationOpt.get();
        Set<Publisher> readSources = application.getConfiguration().getEnabledReadSources();

        Queue<Id> idsToVisit = new LinkedList<>();
        Map<Id, EquivalenceGraph.Adjacents> newAdjacentsMap = new HashMap<>();
        idsToVisit.add(start);

        while(!idsToVisit.isEmpty()) {
            Id id = idsToVisit.poll();
            EquivalenceGraph.Adjacents existing = equivalenceGraph.getAdjacents(id);
            if (!readSources.contains(existing.getRef().getSource()) || newAdjacentsMap.containsKey(id)) {
                continue;
            }
            Set<ResourceRef> newOutgoing = new HashSet<>();
            Set<ResourceRef> newIncoming = new HashSet<>();

            Set<ResourceRef> filteredOutgoing = filterSources(existing.getOutgoingEdges(), readSources);
            Set<ResourceRef> filteredIncoming = filterSources(existing.getIncomingEdges(), readSources);

            for (ResourceRef ref : filteredOutgoing) {
                newOutgoing.add(ref);
                EquivalenceGraph.Adjacents adj = newAdjacentsMap.get(ref.getId());
                newAdjacentsMap.put(ref.getId(), adj.copyWithIncoming(existing.getRef()));
                idsToVisit.add(ref.getId());
            }

            for (ResourceRef ref : filteredIncoming) {
                newIncoming.add(ref);
                EquivalenceGraph.Adjacents adj = newAdjacentsMap.get(ref.getId());
                newAdjacentsMap.put(ref.getId(), adj.copyWithOutgoing(existing.getRef()));
                idsToVisit.add(ref.getId());
            }

            EquivalenceGraph.Adjacents newAdjacents = new EquivalenceGraph.Adjacents(
                    existing.getRef(),
                    new DateTime(DateTimeZones.UTC),
                    newOutgoing,
                    newIncoming
            );
            newAdjacentsMap.put(id, newAdjacents);

        }

        Set<EquivalenceGraph.Adjacents> filteredAdjacentsSet = newAdjacentsMap.values().stream()
                .collect(MoreCollectors.toImmutableSet());
        return EquivalenceGraph.valueOf(filteredAdjacentsSet);
    }

    private Set<ResourceRef> filterSources(Collection<ResourceRef> refs, Set<Publisher> publishers) {
        return refs.stream()
                .filter(ref -> publishers.contains(ref.getSource()))
                .collect(MoreCollectors.toImmutableSet());
    }
}
