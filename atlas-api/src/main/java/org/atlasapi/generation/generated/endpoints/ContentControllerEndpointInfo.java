package org.atlasapi.generation.generated.endpoints;

import java.util.Set;
import com.google.common.collect.ImmutableSet;
import org.atlasapi.generation.model.EndpointClassInfo;
import org.atlasapi.generation.model.Operation;
import org.springframework.web.bind.annotation.RequestMethod;


public class ContentControllerEndpointInfo implements EndpointClassInfo {

    private static final Set<Operation> operations = ImmutableSet.<Operation>builder()
            .add(
                new Operation(RequestMethod.GET, "/4/content/{cid}.*")
            )
            .add(
                new Operation(RequestMethod.GET, "/4/content/{cid}")
            )
            .add(
                new Operation(RequestMethod.GET, "/4/content.*")
            )
            .add(
                new Operation(RequestMethod.GET, "/4/content")
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
    public String description() {
        return " An endpoint for serving pieces of Content. Content can be fetched either by unique ID or by adding filter parameters to the endpoint.  ";
    }

    @Override
    public String rootPath() {
        return "";
    }

}
