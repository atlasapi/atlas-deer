package org.atlasapi.generation.generated.endpoints;

import java.util.Set;

import org.atlasapi.generation.model.EndpointClassInfo;
import org.atlasapi.generation.model.Operation;

import com.google.common.collect.ImmutableSet;
import org.springframework.web.bind.annotation.RequestMethod;

public class ScheduleControllerEndpointInfo implements EndpointClassInfo {

    private static final Set<Operation> operations = ImmutableSet.<Operation>builder()
            .add(
                    new Operation(RequestMethod.GET, "")
            )
            .add(
                    new Operation(RequestMethod.GET, ".*")
            )
            .add(
                    new Operation(RequestMethod.GET, "/{id}.*")
            )
            .add(
                    new Operation(RequestMethod.GET, "/{id}")
            )
            .build();

    @Override
    public Set<Operation> operations() {
        return operations;
    }

    @Override
    public String name() {
        return "schedules";
    }

    @Override
    public String modelKey() {
        return "channelschedule";
    }

    @Override
    public String description() {
        return "";
    }

    @Override
    public String rootPath() {
        return "/4/schedules";
    }

}
