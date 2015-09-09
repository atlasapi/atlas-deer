package org.atlasapi.entity;

import org.atlasapi.equivalence.EquivalenceRef;
import org.atlasapi.serialization.protobuf.CommonProtos;
import org.atlasapi.serialization.protobuf.CommonProtos.Reference;
import org.atlasapi.source.Sources;
import org.joda.time.DateTime;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSet.Builder;


public class IdentifiedSerializer<T extends Identified> {

    public CommonProtos.Identification serialize(T identified) {
        CommonProtos.Identification.Builder id = CommonProtos.Identification.newBuilder()
            .setType(identified.getClass().getSimpleName().toLowerCase());
        if (identified.getId() != null) {
            id.setId(identified.getId().longValue());
        }
        if (identified.getLastUpdated() != null) {
            id.setLastUpdated(new DateTimeSerializer().serialize(identified.getLastUpdated()));
        }
        if (identified.getCanonicalUri() != null) {
            id.setUri(identified.getCanonicalUri());
        }
        if (identified.getCurie() != null) {
            id.setCurie(identified.getCurie());
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
        if (identified.getEquivalenceUpdate() != null) {
            id.setEquivalenceUpdate(CommonProtos.DateTime.newBuilder()
                    .setMillis(identified.getEquivalenceUpdate().getMillis()));
        }
        return id.build();
    }

    public T deserialize(CommonProtos.Identification msg, T identified) {
        if (msg.hasId()) {
            identified.setId(Id.valueOf(msg.getId()));
        }
        if (msg.hasUri()) {
            identified.setCanonicalUri(msg.getUri());
        }
        if (msg.hasCurie()) {
            identified.setCurie(msg.getCurie());
        }
        if (msg.hasLastUpdated()) {
            DateTime lastUpdated = new DateTimeSerializer().deserialize(msg.getLastUpdated());
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
        if (msg.hasEquivalenceUpdate()) {
            identified.setEquivalenceUpdate(new DateTimeSerializer().deserialize(msg.getEquivalenceUpdate()));
        }
        return identified;
    }

    public <I extends Identified.Builder> I deserialize(CommonProtos.Identification msg, I builder) {
        if (msg.hasId()) {
            builder.withId(Id.valueOf(msg.getId()));
        }
        if (msg.hasUri()) {
            builder.withCanonicalUri(msg.getUri());
        }
        if (msg.hasCurie()) {
            builder.withCurie(msg.getCurie());
        }
        if (msg.hasLastUpdated()) {
            DateTime lastUpdated = new DateTimeSerializer().deserialize(msg.getLastUpdated());
            builder.withLastUpdated(lastUpdated);
        }

        Builder<Alias> aliases = ImmutableSet.builder();
        for (CommonProtos.Alias alias : msg.getAliasesList()) {
            aliases.add(new Alias(alias.getNamespace(), alias.getValue()));
        }
        builder.withAliases(aliases.build());

        ImmutableSet.Builder<EquivalenceRef> equivRefs = ImmutableSet.builder();
        for (Reference equivRef : msg.getEquivsList()) {
            equivRefs.add(new EquivalenceRef(Id.valueOf(equivRef.getId()),
                    Sources.fromPossibleKey(equivRef.getSource()).get()
            ));
        }
        builder.withEquivalentTo(equivRefs.build());
        if (msg.hasEquivalenceUpdate()) {
            builder.withEquivalenceUpdate(new DateTimeSerializer().deserialize(msg.getEquivalenceUpdate()));
        }
        return builder;
    }
}
