package org.atlasapi.query.v4.meta.serialization;

import static com.google.common.base.Preconditions.checkNotNull;

import java.lang.reflect.Type;

import org.atlasapi.generation.model.EndpointClassInfo;
import org.atlasapi.query.v4.meta.LinkCreator;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;


public class EndpointClassInfoSerializer implements JsonSerializer<EndpointClassInfo> {
    
    private final LinkCreator linkCreator;

    public EndpointClassInfoSerializer(LinkCreator linkCreator) {
        this.linkCreator = checkNotNull(linkCreator);
    }

    @Override
    public JsonElement serialize(EndpointClassInfo src, Type typeOfSrc,
            JsonSerializationContext context) {
        
        JsonObject json = new JsonObject();
        
        json.add("name", context.serialize(src.name()));
        json.add("model_class_link", context.serialize(linkCreator.createModelLink(src.name())));
        json.add("description", context.serialize(src.description()));
        json.add("root_path", context.serialize(src.rootPath()));
        json.add("operations", context.serialize(src.operations()));
        
        return json;
    }
}
