package org.atlasapi.generation.generated.endpoints;

import java.util.Set;
import com.google.common.collect.ImmutableSet;
import org.atlasapi.generation.model.EndpointClassInfo;
import org.atlasapi.generation.model.Operation;
import org.springframework.web.bind.annotation.RequestMethod;


public class TopicControllerEndpointInfo implements EndpointClassInfo {

    private static final Set<Operation> operations = ImmutableSet.<Operation>builder()
            .add(
                new Operation(RequestMethod.GET, "/4/topics/{tid}.*")
            )
            .add(
                new Operation(RequestMethod.GET, "/4/topics/{tid}")
            )
            .add(
                new Operation(RequestMethod.GET, "/4/topics.*")
            )
            .add(
                new Operation(RequestMethod.GET, "/4/topics")
            )
            .build();

    @Override
    public Set<Operation> operations() {
        return operations;
    }

    @Override
    public String name() {
        return "topic";
    }

    @Override
    public String description() {
        return "";
    }

    @Override
    public String rootPath() {
        return "";
    }

}
