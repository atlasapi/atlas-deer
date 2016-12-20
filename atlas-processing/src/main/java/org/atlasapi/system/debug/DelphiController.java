package org.atlasapi.system.debug;

import java.io.IOException;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.atlasapi.entity.Id;
import org.atlasapi.entity.ResourceRef;
import org.atlasapi.equivalence.EquivalenceGraph;
import org.atlasapi.equivalence.EquivalenceGraphStore;
import org.atlasapi.system.debug.serializers.AdjacentsSerializer;
import org.atlasapi.system.debug.serializers.EquivalenceGraphSerializer;
import org.atlasapi.system.debug.serializers.ResourceRefSerializer;

import com.metabroadcast.common.collect.OptionalMap;
import com.metabroadcast.common.ids.NumberToShortStringCodec;
import com.metabroadcast.common.ids.SubstitutionTableNumberCodec;
import com.metabroadcast.common.webapp.serializers.JodaDateTimeSerializer;

import com.google.common.collect.ImmutableList;
import com.google.common.net.HttpHeaders;
import com.google.gson.FieldNamingPolicy;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;

@Controller
public class DelphiController {

    private static final Logger log = LoggerFactory.getLogger(DelphiController.class);

    private final EquivalenceGraphStore equivalenceGraphStore;
    private final NumberToShortStringCodec codec;
    private final Gson gson;

    private DelphiController(EquivalenceGraphStore equivalenceGraphStore) {
        this.equivalenceGraphStore = equivalenceGraphStore;
        this.codec = SubstitutionTableNumberCodec.lowerCaseOnly();
        this.gson = new GsonBuilder()
                .setFieldNamingPolicy(FieldNamingPolicy.LOWER_CASE_WITH_UNDERSCORES)
                .registerTypeAdapter(DateTime.class, new JodaDateTimeSerializer())
                .registerTypeAdapter(EquivalenceGraph.class, EquivalenceGraphSerializer.create())
                .registerTypeAdapter(EquivalenceGraph.Adjacents.class, AdjacentsSerializer.create())
                .registerTypeAdapter(ResourceRef.class, ResourceRefSerializer.create())
                .create();
    }

    public static DelphiController create(EquivalenceGraphStore equivalenceGraphStore) {
        return new DelphiController(equivalenceGraphStore);
    }

    @RequestMapping(value = "/system/debug/delphi/graph/{id}.json", method = RequestMethod.GET)
    public void listEquivalenceGraph(
            HttpServletRequest request,
            HttpServletResponse response,
            @PathVariable("id") String id
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
}
