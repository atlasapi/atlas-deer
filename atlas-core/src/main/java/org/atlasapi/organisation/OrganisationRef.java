package org.atlasapi.organisation;

import org.atlasapi.content.ContentGroupRef;
import org.atlasapi.entity.Id;
import org.atlasapi.entity.ResourceRef;
import org.atlasapi.entity.ResourceType;
import org.atlasapi.media.entity.Publisher;

public class OrganisationRef extends ResourceRef {

    public OrganisationRef(Id id, Publisher source) {
        super(id, source);
    }

    @Override
    public ResourceType getResourceType() {
        return ResourceType.ORGANISATION;
    }
}
