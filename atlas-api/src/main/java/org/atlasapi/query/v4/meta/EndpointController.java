package org.atlasapi.query.v4.meta;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.Set;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.atlasapi.generation.EndpointClassInfoStore;
import org.atlasapi.generation.model.EndpointClassInfo;
import org.atlasapi.query.v4.meta.serialization.EndpointClassInfoSerializer;
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
    
    private final Gson gson;
    private final EndpointClassInfoStore endpointInfoStore;
    
    public EndpointController(EndpointClassInfoStore endpointInfoStore, LinkCreator linkCreator) {
        this.endpointInfoStore = checkNotNull(endpointInfoStore);
        this.gson = setupSerialization(linkCreator);
    }

    private Gson setupSerialization(LinkCreator linkCreator) {
        return new GsonBuilder()
            .serializeNulls()
            .registerTypeAdapter(EndpointClassInfo.class, new EndpointClassInfoSerializer(linkCreator))
            .registerTypeAdapter(Optional.class, new OptionalSerializer())
            .create();
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
