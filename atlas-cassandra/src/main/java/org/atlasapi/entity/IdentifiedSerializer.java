package org.atlasapi.entity;

import static org.atlasapi.entity.ProtoBufUtils.deserializeDateTime;

import org.atlasapi.content.Identified;
import org.atlasapi.equivalence.EquivalenceRef;
import org.atlasapi.serialization.protobuf.CommonProtos;
import org.atlasapi.serialization.protobuf.CommonProtos.Reference;
import org.atlasapi.source.Sources;
import org.joda.time.DateTime;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSet.Builder;


public class IdentifiedSerializer {

    public CommonProtos.Identification.Builder serialize(Identified identified) {
        CommonProtos.Identification.Builder id = CommonProtos.Identification.newBuilder()
            .setType(identified.getClass().getSimpleName().toLowerCase());
        if (identified.getId() != null) {
            id.setId(identified.getId().longValue());
        }
        if (identified.getLastUpdated() != null) {
            id.setLastUpdated(ProtoBufUtils.serializeDateTime(identified.getLastUpdated()));
        }
        if (identified.getCanonicalUri() != null) {
            id.setUri(identified.getCanonicalUri());
        }
        for (Alias alias : identified.getAliases()) {
            id.addAliases(CommonProtos.Alias.newBuilder()
                    .setNamespace(alias.getNamespace())
                    .setValue(alias.getValue()));
        }
        for (EquivalenceRef equivRef : identified.getEquivalentTo()) {
            id.addEquivs(CommonProtos.Reference.newBuilder()
                .setId(equivRef.getId().longValue())
                .setSource(equivRef.getSource().key())
            );
        }
        return id;
    }
    
    public <I extends Identified> I deserialize(CommonProtos.Identification msg, I identified) {
        if (msg.hasId()) {
            identified.setId(Id.valueOf(msg.getId()));
        }
        if (msg.hasUri()) {
            identified.setCanonicalUri(msg.getUri());
        }
        if (msg.hasLastUpdated()) {
            DateTime lastUpdated = deserializeDateTime(msg.getLastUpdated());
            identified.setLastUpdated(lastUpdated);
        }

        Builder<Alias> aliases = ImmutableSet.builder();
        for (CommonProtos.Alias alias : msg.getAliasesList()) {
            aliases.add(new Alias(alias.getNamespace(), alias.getValue()));
        }
        identified.setAliases(aliases.build());
        
        ImmutableSet.Builder<EquivalenceRef> equivRefs = ImmutableSet.builder();
        for (Reference equivRef : msg.getEquivsList()) {
            equivRefs.add(new EquivalenceRef(Id.valueOf(equivRef.getId()),
                Sources.fromPossibleKey(equivRef.getSource()).get()
            ));
        }
        identified.setEquivalentTo(equivRefs.build());
        return identified;
    }
}
