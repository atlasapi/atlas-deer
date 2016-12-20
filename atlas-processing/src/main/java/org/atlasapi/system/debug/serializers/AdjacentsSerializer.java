package org.atlasapi.system.debug.serializers;

import java.lang.reflect.Type;
import java.util.Set;

import org.atlasapi.entity.ResourceRef;
import org.atlasapi.equivalence.EquivalenceGraph.Adjacents;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;

public class AdjacentsSerializer implements JsonSerializer<Adjacents> {

    private final ResourceRefSerializer resourceRefSerializer = ResourceRefSerializer.create();

    private AdjacentsSerializer() {
    }

    public static AdjacentsSerializer create() {
        return new AdjacentsSerializer();
    }

    @Override
    public JsonElement serialize(
            Adjacents src,
            Type typeOfSrc,
            JsonSerializationContext context
    ) {
        JsonObject json = new JsonObject();

        json.add(
                "resource",
                resourceRefSerializer.serialize(src.getRef(), typeOfSrc, context)
        );
        json.add(
                "created",
                context.serialize(src.getCreated())
        );
        json.add(
                "efferent",
                serializeResources(src.getOutgoingEdges(), typeOfSrc, context)
        );
        json.add(
                "afferent",
                serializeResources(src.getIncomingEdges(), typeOfSrc, context)
        );

        return json;
    }

    private JsonElement serializeResources(
            Set<ResourceRef> resources,
            Type typeOfSrc,
            JsonSerializationContext context
    ) {
        JsonArray jsonArray = new JsonArray();
        for (ResourceRef resourceRef : resources) {
            jsonArray.add(resourceRefSerializer.serialize(resourceRef, typeOfSrc, context));
        }
        return jsonArray;
    }
}
