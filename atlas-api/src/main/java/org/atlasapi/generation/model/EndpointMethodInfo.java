package org.atlasapi.generation.model;

import com.google.common.base.MoreObjects;
import org.springframework.web.bind.annotation.RequestMethod;

import static com.google.common.base.Preconditions.checkNotNull;

public class EndpointMethodInfo implements MethodInfo {

    private final String path;
    private final RequestMethod method;

    public EndpointMethodInfo(String path, RequestMethod method) {
        this.path = checkNotNull(path);
        this.method = checkNotNull(method);
    }

    public String path() {
        return path;
    }

    public RequestMethod method() {
        return method;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(getClass())
                .add("path", path)
                .add("method", method)
                .toString();
    }
}
