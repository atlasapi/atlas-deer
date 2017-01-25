package org.atlasapi.system.debug;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nullable;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.atlasapi.content.Brand;
import org.atlasapi.content.BrandRef;
import org.atlasapi.content.Container;
import org.atlasapi.content.Content;
import org.atlasapi.content.ContentRef;
import org.atlasapi.content.ContentResolver;
import org.atlasapi.content.Episode;
import org.atlasapi.content.Item;
import org.atlasapi.content.Series;
import org.atlasapi.content.SeriesRef;
import org.atlasapi.content.v2.CqlContentStore;
import org.atlasapi.entity.Id;
import org.atlasapi.entity.util.Resolved;
import org.atlasapi.entity.util.WriteException;
import org.atlasapi.entity.util.WriteResult;

import com.metabroadcast.common.ids.NumberToShortStringCodec;
import com.metabroadcast.common.ids.SubstitutionTableNumberCodec;

import com.google.api.client.repackaged.com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
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
import org.springframework.web.bind.annotation.RequestParam;

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
            @PathVariable String id,
            @RequestParam(name = "hierarchy", defaultValue = "false") Boolean hierarchy
    ) throws ExecutionException, InterruptedException, WriteException {
        migrate(id, hierarchy, mongo);
    }

    @RequestMapping(value = "/migrate/astyanax/{id}", method = RequestMethod.POST)
    public void migrate(
            HttpServletRequest request,
            HttpServletResponse response,
            @PathVariable String id,
            @RequestParam(name = "hierarchy", defaultValue = "false") Boolean hierarchy
    ) throws ExecutionException, InterruptedException, WriteException {
        migrate(id, hierarchy, astyanax);
    }

    private void migrate(String encodedId, Boolean writeHierarchy, ContentResolver resolver)
            throws InterruptedException, ExecutionException, WriteException {
        Id id = Id.valueOf(lowercase.decode(encodedId));

        Content content = resolveContent(resolver, id);

        if (content instanceof Container) {
            writeContainer((Container) content, resolver, writeHierarchy);
        } else {
            writeItem((Item) content, resolver, writeHierarchy);
        }
    }

    private Content resolveContent(ContentResolver resolver, Id id) {
        Content content;

        try {
            content = resolver.resolveIds(ImmutableList.of(id))
                    .get()
                    .getResources()
                    .first()
                    .orNull();
        } catch (InterruptedException | ExecutionException e) {
            throw Throwables.propagate(e);
        }

        if (content == null) {
            throw new IllegalArgumentException(String.format("content %s not found", id));
        }

        return content;
    }

    @Nullable
    private Content migrateContent(ContentResolver resolver, @Nullable ContentRef ref) {
        if (ref == null) {
            return null;
        }

        Content series = resolveContent(resolver, ref.getId());
        WriteResult<Content, Content> result;
        try {
            result = cqlStore.writeContent(series);
        } catch (WriteException e) {
            throw Throwables.propagate(e);
        }
        return result.getResource();
    }

    private void writeItem(Item item, ContentResolver resolver, Boolean writeHierarchy) throws WriteException {
        if (item instanceof Episode && writeHierarchy) {
            Episode episode = (Episode) item;

            migrateContent(resolver, episode.getSeriesRef());
            migrateContent(resolver, episode.getContainerRef());
        }

        cqlStore.writeContent(item);
    }

    private void writeContainer(
            Container container,
            ContentResolver resolver,
            Boolean writeHierarchy
    ) throws WriteException {
        if (container instanceof Series && writeHierarchy) {
            Series series = (Series) container;

            BrandRef brandRef = series.getBrandRef();
            migrateContent(resolver, brandRef);
        }

        cqlStore.writeContent(container);

        if (writeHierarchy) {
            if (container instanceof Brand) {
                migrateBrandSeries((Brand) container, resolver);
            }

            container.getItemRefs().stream().forEach(itemRef -> migrateContent(resolver, itemRef));
        }
    }

    private void migrateBrandSeries(Brand brand, ContentResolver resolver) {
        for (SeriesRef seriesRef : brand.getSeriesRefs()) {
            Content maybeSeries = migrateContent(resolver, seriesRef);

            if (maybeSeries instanceof Series) {
                Series series = (Series) maybeSeries;

                series.getItemRefs()
                        .stream()
                        .forEach(itemRef -> migrateContent(resolver, itemRef));
            }
        }
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
