package org.atlasapi.query.v4.meta;

import java.lang.reflect.Type;

import org.atlasapi.generation.model.EndpointClassInfo;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;
import com.metabroadcast.common.properties.Configurer;


public class EndpointClassInfoSerializer implements JsonSerializer<EndpointClassInfo> {

    @Override
    public JsonElement serialize(EndpointClassInfo src, Type typeOfSrc,
            JsonSerializationContext context) {
        
        JsonObject json = new JsonObject();
        
        json.add("name", context.serialize(src.name()));
        json.add("model_class_link", context.serialize(createModelLinkFrom(src.name())));
        json.add("description", context.serialize(src.description()));
        json.add("root_path", context.serialize(src.rootPath()));
        json.add("operations", context.serialize(src.operations()));
        
        return json;
    }

    // TODO public to avoid duplication, and NO OTHER REASON
    public static String createModelLinkFrom(String name) {
        StringBuilder modelLink = new StringBuilder(); 
        
        String platform = Configurer.getPlatform();
        // TODO this is a MASSIVELY ENORMOUS HACK, but along the right lines
        modelLink.append("http://");
        if ("STAGE".equals(platform)) {
            modelLink.append("stage.atlas.metabroadcast.com");
        } else if ("PROD".equals(platform)) {
            modelLink.append("atlas.metabroadcast.com");
        } else {
            modelLink.append("dev.mbst.tv:8080");
        }
        modelLink.append("/4/model_classes/"); 
        modelLink.append(name);
        modelLink.append(".json");
        
        return modelLink.toString();
    }

}
