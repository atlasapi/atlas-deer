package org.atlasapi.organisation;

import org.atlasapi.content.ContentGroupRef;
import org.atlasapi.entity.Id;
import org.atlasapi.entity.ResourceRef;
import org.atlasapi.entity.ResourceType;
import org.atlasapi.media.entity.Publisher;

public class OrganisationRef extends ContentGroupRef {

    public OrganisationRef(Id id, String uri) {
        super(id, uri);
    }

}
