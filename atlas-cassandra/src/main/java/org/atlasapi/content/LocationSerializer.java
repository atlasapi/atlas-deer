package org.atlasapi.content;

import static org.atlasapi.entity.ProtoBufUtils.deserializeDateTime;
import static org.atlasapi.entity.ProtoBufUtils.serializeDateTime;

import java.util.Currency;

import org.atlasapi.channel.Platform;
import org.atlasapi.content.Policy.RevenueContract;
import org.atlasapi.entity.Id;
import org.atlasapi.entity.PlayerRef;
import org.atlasapi.entity.ResourceRef;
import org.atlasapi.entity.ServiceRef;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.serialization.protobuf.CommonProtos;
import org.atlasapi.serialization.protobuf.ContentProtos;
import org.atlasapi.serialization.protobuf.ContentProtos.Location.Builder;

import com.metabroadcast.common.currency.Price;
import com.metabroadcast.common.intl.Countries;


public class LocationSerializer {

    public ContentProtos.Location.Builder serialize(Location location) {
        Builder builder = ContentProtos.Location.newBuilder();
        if (location.getEmbedCode() != null) { builder.setEmbedCode(location.getEmbedCode()); }
        if (location.getEmbedId() != null) { builder.setEmbedId(location.getEmbedId()); }
        if (location.getTransportIsLive() != null) { builder.setTransportIsLive(location.getTransportIsLive()); }
        if (location.getTransportSubType() != null) {
            builder.setTransportSubType(location.getTransportSubType().toString());
        }
        if (location.getTransportType() != null) {
            builder.setTransportType(location.getTransportType().toString());
        }
        if (location.getUri() != null) { builder.setUri(location.getUri()); }
        
        Policy policy = location.getPolicy();
        if (policy == null) {
            return builder;
        }
        
        if (policy.getActualAvailabilityStart() != null) {
            builder.setActualAvailabilityStart(serializeDateTime(policy.getActualAvailabilityStart()));
        }
        if (policy.getAvailabilityStart() != null) {
            builder.setAvailabilityStart(serializeDateTime(policy.getAvailabilityStart()));
        }
        if (policy.getAvailabilityEnd() != null) {
            builder.setAvailabilityEnd(serializeDateTime(policy.getAvailabilityEnd()));
        }
        if (policy.getAvailabilityLength() != null) { builder.setAvailabilityLength(policy.getAvailabilityLength()); }
        builder.addAllAvailableCountries(Countries.toCodes(policy.getAvailableCountries()));

        if (policy.getDrmPlayableFrom() != null) {
            builder.setDrmPlayableFrom(serializeDateTime(policy.getDrmPlayableFrom()));
        }
        if (policy.getNetwork() != null) {
            builder.setNetwork(policy.getNetwork().key());
        }
        if (policy.getPlatform() != null) {
            builder.setPlatform(policy.getPlatform().key());
        }
        if (policy.getServiceRef() != null) {
            ResourceRef serviceRef = policy.getServiceRef();
            long id = serviceRef.getId().longValue();
            String source = serviceRef.getSource().key();
            builder.setServiceId(
                    CommonProtos.Identification.newBuilder().setId(id).setSource(source).build()
            );
        }
        if (policy.getPlayerRef() != null) {
            ResourceRef playerRef = policy.getPlayerRef();
            long id = playerRef.getId().longValue();
            String source = playerRef.getSource().key();
            builder.setPlayerId(
                    CommonProtos.Identification.newBuilder().setId(id).setSource(source).build()
            );
        }
        if (policy.getPrice() != null) {
            builder.setCurrency(policy.getPrice().getCurrency().getCurrencyCode());
            builder.setAmount(policy.getPrice().getAmount());
        }
        if (policy.getRevenueContract() != null) {
            builder.setRevenueContract(policy.getRevenueContract().key());
        }
        
        return builder;
    }
    
    public Location deserialize(ContentProtos.Location msg) {
        Location location = new Location();
        location.setEmbedCode(msg.hasEmbedCode() ? msg.getEmbedCode() : null);
        location.setEmbedId(msg.hasEmbedId() ? msg.getEmbedId() : null);
        location.setTransportIsLive(msg.hasTransportIsLive() ? msg.getTransportIsLive() : null);
        location.setUri(msg.hasUri() ? msg.getUri() : null);
        if (msg.hasTransportType()) {
            location.setTransportType(TransportType.fromString(msg.getTransportType()));
        }
        if (msg.hasTransportSubType()) {
            location.setTransportSubType(TransportSubType.fromString(msg.getTransportSubType()));
        }
        
        Policy policy = new Policy();
        
        if (msg.hasActualAvailabilityStart()) {
            policy.setActualAvailabilityStart(deserializeDateTime(msg.getActualAvailabilityStart()));
        }
        if (msg.hasAvailabilityStart()) {
            policy.setAvailabilityStart(deserializeDateTime(msg.getAvailabilityStart()));
        }
        if (msg.hasAvailabilityEnd()) {
            policy.setAvailabilityEnd(deserializeDateTime(msg.getAvailabilityEnd()));
        }
        policy.setAvailabilityLength(msg.hasAvailabilityLength() ? msg.getAvailabilityLength() : null);
        policy.setAvailableCountries(Countries.fromCodes(msg.getAvailableCountriesList()));

        if (msg.hasDrmPlayableFrom()) {
            policy.setDrmPlayableFrom(deserializeDateTime(msg.getDrmPlayableFrom()));
        }
        if (msg.hasNetwork()) {
            policy.setNetwork(Policy.Network.fromKey(msg.getNetwork()));
        }
        if (msg.hasPlatform()) {
            policy.setPlatform(Policy.Platform.fromKey(msg.getPlatform()));
        }
        if (msg.hasPlayerId()) {
            policy.setPlayerRef(
                    new PlayerRef(
                            Id.valueOf(msg.getPlayerId().getId()),
                            Publisher.fromKey(msg.getPlayerId().getSource()).requireValue()
                    )
            );
        }
        if (msg.hasServiceId()) {
            policy.setServiceRef(
                    new ServiceRef(
                            Id.valueOf(msg.getServiceId().getId()),
                            Publisher.fromKey(msg.getServiceId().getSource()).requireValue()
                    )
            );
        }
        if (msg.hasAmount() && msg.hasCurrency()) {
            policy.setPrice(new Price(
                Currency.getInstance(msg.getCurrency()), 
                msg.getAmount()
            ));
        }
        if (msg.hasRevenueContract()) {
            policy.setRevenueContract(RevenueContract.fromKey(msg.getRevenueContract()));
        }
        location.setPolicy(policy);
        return location;
    }
    
}
