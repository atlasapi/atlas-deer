package org.atlasapi.output;

import java.io.IOException;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import com.google.common.net.HttpHeaders;
import com.metabroadcast.common.media.MimeType;


public class ResponseWriterFactory {
    
    private final Map<String, MimeType> extensionMap = ImmutableMap.of(".json", MimeType.APPLICATION_JSON);

    public ResponseWriter writerFor(HttpServletRequest request, HttpServletResponse response)
            throws IOException, UnsupportedFormatException, NotAcceptableException {
        ResponseWriter writerFromExtension = writerFromExtension(request, response);
        response.addHeader("Access-Control-Allow-Origin", "*");
        response.setHeader("Cache-Control", "600");
        response.setHeader("Vary", "Origin");
        
        if (writerFromExtension != null) {
            return writerFromExtension;
        }
        return writerFromAcceptHeader(request, response);
    }

    private ResponseWriter writerFromAcceptHeader(HttpServletRequest request, HttpServletResponse response)
            throws NotAcceptableException {
        String acceptHeader = request.getHeader(HttpHeaders.ACCEPT);
        if (acceptHeader != null) {
            Optional<MimeType> mimeType = MimeType.possibleFromString(acceptHeader);
            if(mimeType.isPresent() && extensionMap.values().contains(mimeType.get())) {
                return new JsonResponseWriter(request, response);
            }
        }
        throw new NotAcceptableException("Cannot generate content for type " + acceptHeader);
    }

    private ResponseWriter writerFromExtension(HttpServletRequest request, HttpServletResponse response) throws UnsupportedFormatException {
        String requestUri = request.getRequestURI();
        String extension = extensionFromUri(requestUri);
        if (extension != null) {
            MimeType mimeType = extensionMap.get(extension);
            if (mimeType == null) {
                throw new UnsupportedFormatException(extension);
            }
            return new JsonResponseWriter(request, response);
        }
        return null;
    }
    
    
    private String extensionFromUri(String requestUri) {
        String resource = requestUri.substring(requestUri.lastIndexOf("/"));
        int suffixStart = resource.indexOf(".");
        if (suffixStart >= 0) {
            return resource.substring(suffixStart);
        }
        return null;
    }

}
