package org.atlasapi.system.bootstrap.workers;

import org.atlasapi.messaging.ResourceUpdatedMessage;
import org.atlasapi.messaging.v3.EntityUpdatedMessage;

public class EntityUpdatedLegacyMessageSerializer extends LegacyMessageSerializer<EntityUpdatedMessage, ResourceUpdatedMessage> {

    public EntityUpdatedLegacyMessageSerializer() {
        super(EntityUpdatedMessage.class);
    }

    @Override
    protected ResourceUpdatedMessage transform(EntityUpdatedMessage leg) {
        return new ResourceUpdatedMessage(
                leg.getMessageId(),
                leg.getTimestamp(),
                resourceRef(leg));
    }
    
}
