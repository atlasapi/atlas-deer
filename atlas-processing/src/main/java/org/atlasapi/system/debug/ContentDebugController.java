package org.atlasapi.system.debug;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.IOException;
import java.lang.reflect.Type;

import javax.servlet.http.HttpServletResponse;

import com.metabroadcast.common.base.Maybe;
import org.atlasapi.content.*;
import org.atlasapi.entity.Id;
import org.atlasapi.entity.util.Resolved;
import org.atlasapi.media.entity.Publisher;
import org.joda.time.DateTime;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;
import org.springframework.web.bind.annotation.RequestParam;

@Controller
public class ContentDebugController {

    private final Gson gson = new GsonBuilder().registerTypeAdapter(DateTime.class, new JsonSerializer<DateTime>() {
        @Override
        public JsonElement serialize(DateTime src, Type typeOfSrc, JsonSerializationContext context) {
            return new JsonPrimitive(src.toString());
        }
    }).create();

    private final ContentResolver resolver;
    private final EquivalentContentStore equivalentContentStore;

    public ContentDebugController(ContentResolver resolver, EquivalentContentStore equivalentContentStore) {
        this.resolver = checkNotNull(resolver);
        this.equivalentContentStore = checkNotNull(equivalentContentStore);
    }

    @RequestMapping("/system/debug/content/{id}")
    public void printContent(@PathVariable("id") Long id, final HttpServletResponse response) {
        ImmutableList<Id> ids = ImmutableList.of(Id.valueOf(id));
        Futures.addCallback(resolver.resolveIds(ids), new FutureCallback<Resolved<Content>>() {

            @Override
            public void onSuccess(Resolved<Content> result) {
                try {
                    Content content = result.getResources().first().orNull();
                    gson.toJson(content, response.getWriter());
                } catch (Exception e) {
                    Throwables.propagate(e);
                }
            }

            @Override
            public void onFailure(Throwable t) {
                try {
                    t.printStackTrace(response.getWriter());
                } catch (IOException e) {
                    Throwables.propagate(e);
                }
            }
        });
    }

    @RequestMapping("/system/debug/content/{id}/update-equiv")
    public void forceEquivUpdate(@PathVariable("id") Long id, @RequestParam(value = "publisher", required = true) String publisherKey,
                                 final HttpServletResponse response) throws IOException {
        try {
            Maybe<Publisher> publisherMaybe = Publisher.fromKey(publisherKey);
            if (publisherMaybe.isNothing()) {
                response.setStatus(400);
                response.getWriter().write("Supply a valid publisher key");
                response.flushBuffer();
                return;
            }

            Publisher publisher = publisherMaybe.requireValue();
            ItemRef ref = new ItemRef(Id.valueOf(id), publisher, "sortKey", DateTime.now());
            equivalentContentStore.updateContent(ref);

            response.setStatus(200);
            response.getWriter().write("Updated content equivalence for " + id.toString());
            response.flushBuffer();
        } catch (Throwable t) {
            t.printStackTrace(response.getWriter());
        }
    }
}
