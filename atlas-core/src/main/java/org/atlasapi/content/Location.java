/* Copyright 2009 British Broadcasting Corporation
   Copyright 2009 Meta Broadcast Ltd

Licensed under the Apache License, Version 2.0 (the "License"); you
may not use this file except in compliance with the License. You may
obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
implied. See the License for the specific language governing
permissions and limitations under the License. */

package org.atlasapi.content;

import java.util.List;
import java.util.Set;

import org.atlasapi.entity.Identified;
import org.atlasapi.meta.annotations.FieldName;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSortedSet;
import com.google.common.collect.Iterables;

/**
 * @author Robert Chatley (robert@metabroadcast.com)
 * @author Lee Denison (lee@metabroadcast.com)
 */
public class Location extends Identified {

    private boolean available = true;

    private Boolean transportIsLive;

    private TransportSubType transportSubType;

    private TransportType transportType;
    
    private String uri;

    private String embedCode;
    
    private String embedId;
    
    private Policy policy;

    private Boolean requiredEncryption;

    private Set<String> subtitledLanguages;

    private Double vat;
    
    @FieldName("policy")
    public Policy getPolicy() { 
        return this.policy; 
    }

    @FieldName("transport_is_live")
    public Boolean getTransportIsLive() {
        return this.transportIsLive;
    }
    
    @FieldName("transport_sub_type")
    public TransportSubType getTransportSubType() { 
        return this.transportSubType; 
    }

    @FieldName("transport_type")
    public TransportType getTransportType() { 
        return this.transportType; 
    }

    @FieldName("available")
    public boolean getAvailable() {
    	return available;
    }
    
    public void setAvailable(boolean available) {
    	this.available = available;
	}
    
    public void setPolicy(Policy policy) { 
        this.policy = policy; 
    }

    public void setTransportIsLive(Boolean transportIsLive) {
        this.transportIsLive = transportIsLive;
    }
    
    public void setTransportSubType(TransportSubType transportSubType) {
		this.transportSubType = transportSubType; 
    }

    public void setTransportType(TransportType transportType) {
		this.transportType = transportType; 
    }

    @FieldName("uri")
    public String getUri() {
		return uri;
	}
    
    public void setUri(String uri) {
		this.uri = uri;
	}
    
    @FieldName("embed_code")
    public String getEmbedCode() {
		return embedCode;
	}
    
    @FieldName("embed_id") 
    public String getEmbedId() {
        return embedId;
    }
    
    public void setEmbedCode(String embedCode) {
		this.embedCode = embedCode;
	}
    
    public void setEmbedId(String embedId) {
        this.embedId = embedId;
    }

    public Boolean isAvailable() {
        return AVAILABLE.apply(this);
    }

    @FieldName("vat")
    public Double getVat() {
        return vat;
    }

    public void setVat(Double vat) {
        this.vat = vat;
    }

    @FieldName("subtitledLanguages")
    public Set<String> getSubtitledLanguages() {
        return subtitledLanguages;
    }

    public void setSubtitledLanguages(Iterable<String> subtitledLanguages) {
        this.subtitledLanguages = ImmutableSortedSet.copyOf(subtitledLanguages);
    }
    public void addSubtitledLanguages(String subtitledLanguage) {
        addSubtitledLanguages(ImmutableList.of(subtitledLanguage));
    }

    public void addSubtitledLanguages(Iterable<String> subtitledLanguages) {
        setSubtitledLanguages(Iterables.concat(this.subtitledLanguages, ImmutableList.copyOf(subtitledLanguages)));
    }
    @FieldName("requiredEncryption")
    public Boolean getRequiredEncryption() {
        return requiredEncryption;
    }

    public void setRequiredEncryption(Boolean requiredEncryption) {
        this.requiredEncryption = requiredEncryption;
    }

    public LocationSummary toSummary() {
        return new LocationSummary(
                available,
                uri,
                policy != null ? policy.getAvailabilityStart() : null,
                policy != null ? policy.getAvailabilityEnd(): null
        );
    }
    
    public Location copy() {
        Location copy = new Location();
        Identified.copyTo(this, copy);
        copy.available = available;
        copy.embedCode = embedCode;
        copy.embedId = embedId;
        if (policy != null) {
            copy.policy = policy.copy();
        }
        copy.transportIsLive = transportIsLive;
        copy.transportSubType = transportSubType;
        copy.transportType = transportType;
        copy.uri = uri;
        copy.requiredEncryption = requiredEncryption;
        copy.vat = vat;
        copy.subtitledLanguages = subtitledLanguages;
        return copy;
    }
    
    public static final Function<Location, Location> COPY = new Function<Location, Location>() {
        @Override
        public Location apply(Location input) {
            return input.copy();
        }
    };

    public static final Predicate<Location> AVAILABLE = l -> {
        DateTime now = DateTime.now(DateTimeZone.UTC);
        return l.getAvailable()
                && (
                l.getPolicy() == null
                || (
                        (l.getPolicy().getAvailabilityStart() == null || now.isAfter(l.getPolicy().getAvailabilityStart())))
                        && (l.getPolicy().getAvailabilityEnd() == null || now.isBefore(l.getPolicy().getAvailabilityEnd()))
                );
    };
}
