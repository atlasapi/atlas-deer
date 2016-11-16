package org.atlasapi.query.v4.content.v2;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.atlasapi.content.Content;
import org.atlasapi.content.ContentResolver;
import org.atlasapi.content.v2.CqlContentStore;
import org.atlasapi.entity.Id;
import org.atlasapi.entity.util.Resolved;
import org.atlasapi.entity.util.WriteException;

import com.metabroadcast.common.ids.NumberToShortStringCodec;
import com.metabroadcast.common.ids.SubstitutionTableNumberCodec;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.Futures;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSerializer;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;

import static com.google.common.base.Preconditions.checkNotNull;

@Controller
@RequestMapping("/system/debug/cql-content")
public class CqlContentDebugController {

    private final NumberToShortStringCodec lowercase = SubstitutionTableNumberCodec.lowerCaseOnly();
    private final Gson gson = new GsonBuilder()
            .registerTypeAdapter(
                    DateTime.class,
                    (JsonSerializer<DateTime>) (src, typeOfSrc, context) -> new JsonPrimitive(src.toString())
            )
            .registerTypeAdapter(
                    Interval.class,
                    (JsonSerializer<Interval>) (src, typeOfSrc, context) ->
                            new JsonPrimitive(src.toString())
            )
            .create();

    private final ContentResolver mongo;
    private final ContentResolver astyanax;
    private final CqlContentStore cqlStore;

    public CqlContentDebugController(
            ContentResolver mongo,
            ContentResolver astyanax,
            CqlContentStore cqlStore
    ) {
        this.mongo = checkNotNull(mongo);
        this.astyanax = checkNotNull(astyanax);
        this.cqlStore = checkNotNull(cqlStore);
    }

    @RequestMapping(value = "/migrate/owl/{id}", method = RequestMethod.POST)
    public void migrateFromOwl(
            HttpServletRequest request,
            HttpServletResponse response,
            @PathVariable String id
    ) throws ExecutionException, InterruptedException, WriteException {
        migrate(id, mongo);
    }

    @RequestMapping(value = "/migrate/astyanax/{id}", method = RequestMethod.POST)
    public void migrate(
            HttpServletRequest request,
            HttpServletResponse response,
            @PathVariable String id
    ) throws ExecutionException, InterruptedException, WriteException {
        migrate(id, astyanax);
    }

    private void migrate(String encodedId, ContentResolver resolver)
            throws InterruptedException, ExecutionException, WriteException {
        Id id = Id.valueOf(lowercase.decode(encodedId));
        Resolved<Content> content = resolver.resolveIds(ImmutableList.of(id)).get();
        cqlStore.writeContent(Iterables.getOnlyElement(content.getResources()));
    }

    @RequestMapping("/{id}")
    public void resolve(
            HttpServletRequest request,
            HttpServletResponse response,
            @PathVariable String id
    ) throws Exception {
        Long decodedId = lowercase.decode(id).longValue();
        ImmutableList<Id> ids = ImmutableList.of(Id.valueOf(decodedId));
        Resolved<Content> result = Futures.get(
                cqlStore.resolveIds(ids), 1, TimeUnit.MINUTES, Exception.class
        );
        Content content = result.getResources().first().orNull();
        gson.toJson(content, response.getWriter());
    }
}
