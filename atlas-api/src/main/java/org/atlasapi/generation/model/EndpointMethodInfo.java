package org.atlasapi.generation.model;

import com.google.common.base.Objects;
import org.springframework.web.bind.annotation.RequestMethod;

import static com.google.common.base.Preconditions.checkNotNull;

public class EndpointMethodInfo implements MethodInfo {

    private final String path;
    private final RequestMethod method;

    // TODO is builder needed? remove if more fields not added
    //    public static Builder builder() {
    //        return new Builder();
    //    }

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
        return Objects.toStringHelper(getClass())
                .add("path", path)
                .add("method", method)
                .toString();
    }
    //
    //    public static class Builder {
    //
    //        private String path;
    //        private RequestMethod method;
    //
    //        private Builder() { }
    //
    //        public EndpointMethodInfo build() {
    //            return new EndpointMethodInfo(path, method);
    //        }
    //
    //        public Builder withPath(String path) {
    //            this.path = path;
    //            return this;
    //        }
    //
    //        public Builder withMethod(RequestMethod method) {
    //            this.method = method;
    //            return this;
    //        }
    //    }
}
