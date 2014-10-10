package org.atlasapi.query.v4.meta;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.Set;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.atlasapi.generation.EndpointClassInfoStore;
import org.atlasapi.generation.model.EndpointClassInfo;
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
import com.metabroadcast.common.webapp.serializers.OptionalSerializer;

@Controller
public class EndpointController {
    
    private final Gson gson = new GsonBuilder()
            .serializeNulls()
            .registerTypeAdapter(JsonCollectionWrapper.class, new JsonCollectionWrapperSerializer<EndpointClassInfo>())
            .registerTypeAdapter(EndpointClassInfo.class, new EndpointClassInfoSerializer())
            .registerTypeAdapter(Optional.class, new OptionalSerializer())
            .create();
    private final EndpointClassInfoStore endpointInfoStore;
    
    public EndpointController(EndpointClassInfoStore endpointInfoStore) {
        this.endpointInfoStore = checkNotNull(endpointInfoStore);
    }
    
    @RequestMapping({"/4/endpoints.*", "/4/endpoints"})
    public void fetchAllEndpointInfo(HttpServletRequest request, HttpServletResponse response) throws IOException { 
        Set<EndpointClassInfo> allEndpointInformation = endpointInfoStore.allEndpointInformation();
        
        response.setContentType(MimeType.APPLICATION_JSON.toString());
        response.setStatus(HttpStatusCode.OK.code());
        response.setCharacterEncoding(Charsets.UTF_8.toString());

        OutputStreamWriter writer = new OutputStreamWriter(response.getOutputStream(), Charsets.UTF_8);
        
        try {
            gson.toJson(allEndpointInformation, new TypeToken<Set<EndpointClassInfo>>(){}.getType(), writer);
        } finally {
            Flushables.flushQuietly(writer);
        }
    }
    
    @RequestMapping({"/4/endpoints/{key}.*", "/4/endpoints/{key}"})
    public void fetchSingleEndpointInfo(HttpServletRequest request, HttpServletResponse response,
            @PathVariable("key") String key) throws IOException { 
        Optional<EndpointClassInfo> endpointInfo = endpointInfoStore.endpointInfoFor(key);
        if (!endpointInfo.isPresent()) {
            response.setStatus(HttpStatusCode.NOT_FOUND.code());
            return;
        }
        
        response.setContentType(MimeType.APPLICATION_JSON.toString());
        response.setStatus(HttpStatusCode.OK.code());
        response.setCharacterEncoding(Charsets.UTF_8.toString());

        OutputStreamWriter writer = new OutputStreamWriter(response.getOutputStream(), Charsets.UTF_8);
        
        try {
            gson.toJson(endpointInfo.get(), new TypeToken<EndpointClassInfo>(){}.getType(), writer);
        } finally {
            Flushables.flushQuietly(writer);                
        }
    }
}
