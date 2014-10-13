package org.atlasapi.query.v4.meta;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.Set;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.atlasapi.generation.ModelClassInfoStore;
import org.atlasapi.generation.model.FieldInfo;
import org.atlasapi.generation.model.ModelClassInfo;
import org.atlasapi.query.v4.meta.serialization.FieldInfoSerializer;
import org.atlasapi.query.v4.meta.serialization.ModelClassInfoSerializer;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;

import com.google.common.base.Charsets;
import com.google.common.base.Optional;
import com.google.common.io.Flushables;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;
import com.metabroadcast.common.http.HttpStatusCode;
import com.metabroadcast.common.media.MimeType;
import com.metabroadcast.common.webapp.json.JsonWrapper;
import com.metabroadcast.common.webapp.json.JsonWrapperSerializer;
import com.metabroadcast.common.webapp.serializers.OptionalSerializer;

@Controller
public class ModelController {
    
    private final Gson gson;
    private final ModelClassInfoStore classInfoStore;
    
    public ModelController(ModelClassInfoStore classInfoStore, LinkCreator linkCreator) {
        this.classInfoStore = checkNotNull(classInfoStore);
        this.gson = setupSerialization(linkCreator);
    }

    private Gson setupSerialization(LinkCreator linkCreator) {
        return new GsonBuilder()
        .serializeNulls()
        .registerTypeAdapter(JsonWrapper.class, new JsonWrapperSerializer())
        .registerTypeAdapter(ModelClassInfo.class, new ModelClassInfoSerializer())
        .registerTypeAdapter(FieldInfo.class, new FieldInfoSerializer(linkCreator))
        .registerTypeAdapter(Optional.class, new OptionalSerializer())
        .create();
    }
    
    // TODO this is hacked together for prototyping purposes. TIDY IT UP
    @RequestMapping({"/4/model_classes.*", "/4/model_classes"})
    public void fetchAllModelInfo(HttpServletRequest request, HttpServletResponse response) throws IOException { 
        Iterable<ModelClassInfo> allClassInformation = classInfoStore.allClassInformation();
        
        response.setContentType(MimeType.APPLICATION_JSON.toString());
        response.setStatus(HttpStatusCode.OK.code());
        response.setCharacterEncoding(Charsets.UTF_8.toString());

        OutputStreamWriter writer = new OutputStreamWriter(response.getOutputStream(), Charsets.UTF_8);
        
        try {
            gson.toJson(allClassInformation, new TypeToken<Set<ModelClassInfo>>(){}.getType(), writer);
        } finally {
            Flushables.flushQuietly(writer);
        }
    }
    
    @RequestMapping({"/4/model_classes/{key}.*", "/4/model_classes/{key}"})
    public void fetchSingleModelInfo(HttpServletRequest request, HttpServletResponse response,
            @PathVariable("key") String key) throws IOException { 
        Optional<ModelClassInfo> classInfo = classInfoStore.classInfoFor(key);
        if (!classInfo.isPresent()) {
            response.setStatus(HttpStatusCode.NOT_FOUND.code());
            return;
        }
        
        response.setContentType(MimeType.APPLICATION_JSON.toString());
        response.setStatus(HttpStatusCode.OK.code());
        response.setCharacterEncoding(Charsets.UTF_8.toString());

        OutputStreamWriter writer = new OutputStreamWriter(response.getOutputStream(), Charsets.UTF_8);
        
        try {
            gson.toJson(classInfo.get(), new TypeToken<ModelClassInfo>(){}.getType(), writer);
        } finally {
            Flushables.flushQuietly(writer);                
        }
    }
}
