package org.atlasapi.generation.generated.endpoints;

import java.util.Set;
import com.google.common.collect.ImmutableSet;
import org.atlasapi.generation.model.EndpointClassInfo;
import org.atlasapi.generation.model.Operation;
import org.springframework.web.bind.annotation.RequestMethod;


public class ChannelControllerEndpointInfo implements EndpointClassInfo {

    private static final Set<Operation> operations = ImmutableSet.<Operation>builder()
            .add(
                new Operation(RequestMethod.GET, "")
            )
            .add(
                new Operation(RequestMethod.GET, ".*")
            )
            .add(
                new Operation(RequestMethod.GET, "/{cid}.*")
            )
            .add(
                new Operation(RequestMethod.GET, "/{cid}")
            )
            .build();

    @Override
    public Set<Operation> operations() {
        return operations;
    }

    @Override
    public String name() {
        return "channels";
    }

    @Override
    public String description() {
        return "";
    }

    @Override
    public String rootPath() {
        return "/4/channels";
    }

}
