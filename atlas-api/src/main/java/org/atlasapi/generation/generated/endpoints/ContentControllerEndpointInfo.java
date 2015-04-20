package org.atlasapi.generation.generated.endpoints;

import java.util.Set;
import com.google.common.collect.ImmutableSet;
import org.atlasapi.generation.model.EndpointClassInfo;
import org.atlasapi.generation.model.Operation;
import org.springframework.web.bind.annotation.RequestMethod;


public class ContentControllerEndpointInfo implements EndpointClassInfo {

    private static final Set<Operation> operations = ImmutableSet.<Operation>builder()
            .add(
                new Operation(RequestMethod.GET, "/{id}.*")
            )
            .add(
                new Operation(RequestMethod.GET, "/{id}")
            )
            .add(
                new Operation(RequestMethod.GET, ".*")
            )
            .add(
                new Operation(RequestMethod.GET, "")
            )
            .build();

    @Override
    public Set<Operation> operations() {
        return operations;
    }

    @Override
    public String name() {
        return "content";
    }

    @Override
    public String modelKey() {
        return "content";
    }

    @Override
    public String description() {
        return " An endpoint for serving pieces of Content. Content can be fetched either by unique ID or by adding filter parameters to the endpoint.  ";
    }

    @Override
    public String rootPath() {
        return "/4/content";
    }

}
