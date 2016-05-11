package org.atlasapi.system.legacy;

import org.atlasapi.content.Actor;
import org.atlasapi.content.CrewMember;

import java.util.Objects;


public class LegacyCrewMemberTransformer extends BaseLegacyResourceTransformer<org.atlasapi.media.entity.CrewMember, CrewMember>{


    private void addCrewMember(org.atlasapi.media.entity.CrewMember legacy, CrewMember translated) {
        translated.withName(legacy.name());

        // unfortunately legacy role doesn't set a value (not even UNKNOWN) by default
        if (null != legacy.role()) {
            translated.withRole(CrewMember.Role.fromKey(legacy.role().key()));
        }
    }

    private void addActor(org.atlasapi.media.entity.Actor legacy, Actor translated) {
        translated.withCharacter(legacy.character());
    }

    public Actor translateLegacyActor(org.atlasapi.media.entity.Actor legacy) {
        Actor translated = new Actor(legacy.getCanonicalUri(), legacy.getCurie(), legacy.publisher());
        addIdentified(legacy, translated);
        addCrewMember(legacy, translated);
        addActor(legacy, translated);

        return translated;
    }


    public CrewMember translateLegacyCrewMember(org.atlasapi.media.entity.CrewMember legacy) {

        // it is possible that in the future there will be other subclasses of CrewMember that add
        // additional fields, this is to identify when we're dealing with a subclassing situation as we
        // might lose information
        if (legacy.getClass() != org.atlasapi.media.entity.CrewMember.class) {
            log.warn("Generic legacy CrewMember type: class={} id={} name='{}' role='{}', best effort migration",
                    Objects.toString(legacy.getClass().getName(), "unidentifed"),
                    Objects.toString(legacy.getId(), "?"),
                    Objects.toString(legacy.name(), "?"),
                    Objects.toString(legacy.role(), "?")
            );
        }

        CrewMember translated = new CrewMember(legacy.getCanonicalUri(), legacy.getCurie(), legacy.publisher());
        addIdentified(legacy, translated);
        addCrewMember(legacy, translated);

        return translated;
    }

    @Override
    public CrewMember apply(org.atlasapi.media.entity.CrewMember input) {
        // we carry IDs through from Owl to Deer, reject anything without one
        if (null == input.getId()) {
            return null;
        }

        if (input instanceof org.atlasapi.media.entity.Actor) {
            return translateLegacyActor((org.atlasapi.media.entity.Actor) input);
        } else if(null != input) {
            return translateLegacyCrewMember(input);
        } else {
            return null;
        }
    }
}
