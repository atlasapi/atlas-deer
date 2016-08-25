package org.atlasapi.equivalence;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.atlasapi.entity.Id;
import org.atlasapi.entity.ResourceRef;
import org.atlasapi.entity.ResourceRefSerializer;
import org.atlasapi.entity.Serializer;
import org.atlasapi.equivalence.EquivalenceGraph.Adjacents;
import org.atlasapi.serialization.protobuf.CommonProtos;
import org.atlasapi.serialization.protobuf.CommonProtos.Reference;
import org.atlasapi.serialization.protobuf.EquivProtos;
import org.atlasapi.serialization.protobuf.EquivProtos.Adjacency;
import org.atlasapi.serialization.protobuf.EquivProtos.EquivGraph;

import com.metabroadcast.common.time.DateTimeZones;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import org.joda.time.DateTime;

/**
 * For legacy reasons we need to refer to outgoing edges as efferent and incoming edges as
 * afferent. This is because {@link org.atlasapi.equivalence.EquivalenceGraph.Adjacents} used to
 * use those names and since it is serialised into / deserialised from protobuf there will be
 * database entries that still use those names.
 */
public class EquivalenceGraphSerializer implements Serializer<EquivalenceGraph, ByteBuffer> {

    ResourceRefSerializer serializer = new ResourceRefSerializer();

    @Override
    public ByteBuffer serialize(EquivalenceGraph src) {
        EquivProtos.EquivGraph.Builder dest = EquivProtos.EquivGraph.newBuilder();
        dest.setUpdated(serialize(src.getUpdated()));
        for (Adjacents adjs : src.getAdjacencyList().values()) {
            dest.addAdjacency(serialize(adjs));
        }
        return ByteBuffer.wrap(dest.build().toByteArray());
    }

    private Adjacency.Builder serialize(Adjacents adjs) {
        Adjacency.Builder dest = Adjacency.newBuilder();

        dest.setRef(serializer.serialize(adjs.getRef()));
        dest.setCreated(serialize(adjs.getCreated()));

        for (ResourceRef outgoingEdge : adjs.getOutgoingEdges()) {
            dest.addEfferent(serializer.serialize(outgoingEdge));
        }
        for (ResourceRef incomingEdge : adjs.getIncomingEdges()) {
            dest.addAfferent(serializer.serialize(incomingEdge));
        }

        return dest;
    }

    private CommonProtos.DateTime.Builder serialize(DateTime dateTime) {
        return CommonProtos.DateTime.newBuilder()
                .setMillis(dateTime.toDateTime(DateTimeZones.UTC).getMillis());
    }

    @Override
    public EquivalenceGraph deserialize(ByteBuffer dest) {
        try {
            ByteString bytes = ByteString.copyFrom(dest);
            EquivGraph buffer = EquivProtos.EquivGraph.parseFrom(bytes);
            return new EquivalenceGraph(
                    deserialize(buffer.getAdjacencyList()),
                    deserialize(buffer.getUpdated())
            );
        } catch (InvalidProtocolBufferException e) {
            throw new RuntimeException(e);
        }
    }

    private Map<Id, Adjacents> deserialize(List<Adjacency> src) {
        ImmutableMap.Builder<Id, Adjacents> adjacencyList = ImmutableMap.builder();
        for (Adjacency adjacency : src) {
            Adjacents adjs = deserialize(adjacency);
            adjacencyList.put(adjs.getId(), adjs);
        }
        return adjacencyList.build();
    }

    private Adjacents deserialize(Adjacency adjacency) {
        ResourceRef subj = serializer.deserialize(adjacency.getRef());
        DateTime deserialize = deserialize(adjacency.getCreated());
        Set<ResourceRef> outgoingEdges = deserializeRefs(adjacency.getEfferentList());
        Set<ResourceRef> incomingEdges = deserializeRefs(adjacency.getAfferentList());
        return new Adjacents(subj, deserialize, outgoingEdges, incomingEdges);
    }

    private Set<ResourceRef> deserializeRefs(List<Reference> refList) {
        ImmutableSet.Builder<ResourceRef> refs = ImmutableSet.builder();
        for (Reference reference : refList) {
            refs.add(serializer.deserialize(reference));
        }
        return refs.build();
    }

    private DateTime deserialize(CommonProtos.DateTime dateTime) {
        return new DateTime(dateTime.getMillis(), DateTimeZones.UTC);
    }

}
