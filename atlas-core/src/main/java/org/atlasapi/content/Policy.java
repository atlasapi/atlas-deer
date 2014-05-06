/* Copyright 2010 Meta Broadcast Ltd

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

import java.util.Set;

import org.joda.time.DateTime;

import com.google.common.collect.Sets;
import com.metabroadcast.common.currency.Price;
import com.metabroadcast.common.intl.Country;

public class Policy extends Identified {

	private DateTime availabilityStart;

	private DateTime availabilityEnd;
	
    private DateTime drmPlayableFrom;
    
    private Set<Country> availableCountries = Sets.newHashSet();

	private Integer availabilityLength;
	
	private RevenueContract revenueContract;
	
	private Price price;
	
	private Platform platform;
	
	private Network network;
	
	private DateTime actualAvailabilityStart;
    
    public Set<Country> getAvailableCountries() {
		return availableCountries;
	}

	public void setAvailableCountries(Set<Country> availableCountries) {
		this.availableCountries = availableCountries;
	}

	public void addAvailableCountry(Country country) {
    	if (availableCountries == null) {
    		availableCountries = Sets.newHashSet();
    	}
    	availableCountries.add(country);
    }
    
    public DateTime getAvailabilityStart() { 
        return availabilityStart;
    }

    public DateTime getDrmPlayableFrom() { 
        return drmPlayableFrom;
    }
    
    public DateTime getAvailabilityEnd() {
		return availabilityEnd;
	}
    
    public Platform getPlatform() {
        return platform;
    }
    
    public Network getNetwork() {
        return network;
    }
    
    public DateTime getActualAvailabilityStart() { 
        return actualAvailabilityStart;
    }
    
    public void setAvailabilityEnd(DateTime availabilityEnd) {
		this.availabilityEnd = availabilityEnd;
	}
    
    public void setAvailabilityStart(DateTime availabilityStart) {
        this.availabilityStart = availabilityStart;
    }

    public void setDrmPlayableFrom(DateTime drmPlayableFrom) {
        this.drmPlayableFrom = drmPlayableFrom;
    }

	public Policy withAvailabilityStart(DateTime end) {
		setAvailabilityStart(end);
		return this;
	}
	
	public Policy withAvailabilityEnd(DateTime start) {
		setAvailabilityEnd(start);
		return this;
	}
	
	public void setRevenueContract(RevenueContract revenueContract) {
        this.revenueContract = revenueContract;
    }
	
	public Policy withRevenueContract(RevenueContract revenueContract) {
	    setRevenueContract(revenueContract);
	    return this;
	}
	
	public RevenueContract getRevenueContract() {
        return revenueContract;
    }
	
	public void setPrice(Price price) {
        this.price = price;
    }
	
	public void setPlatform(Platform platform) {
		this.platform = platform;
	}
	
	public void setNetwork(Network network) {
	    this.network = network;
	}
    
    public void setActualAvailabilityStart(DateTime actualAvailabilityStart) {
        this.actualAvailabilityStart = actualAvailabilityStart;
    }
	
	public Policy withPrice(Price price) {
	    setPrice(price);
	    return this;
	}
	
	public Price getPrice() {
        return price;
    }

	public Policy withDrmPlayableFrom(DateTime from) {
		setDrmPlayableFrom(from);
		return this;
	}

	public void setAvailabilityLength(Integer seconds) {
		this.availabilityLength = seconds;
	}
	
	public Integer getAvailabilityLength() {
		return availabilityLength;
	}

	public Policy withAvailableCountries(Country... countries) {
		setAvailableCountries(Sets.newHashSet(countries));
		return this;
	}
	
	public Policy withPlatform(Platform platform) {
		setPlatform(platform);
		return this;
	}
	
	public enum RevenueContract {
	    PAY_TO_BUY,
	    PAY_TO_RENT,
	    SUBSCRIPTION,
	    FREE_TO_VIEW,
	    VOLUNTARY_DONATION,
	    PRIVATE;
	    
	    public String key() {
	        return name().toLowerCase();
	    }
	    
	    public static RevenueContract fromKey(String key) {
	        for (RevenueContract contract: values()) {
	            if (contract.key().equals(key)) {
	                return contract;
	            }
	        }
	        return FREE_TO_VIEW;
	    }
	}
	
	public Policy copy() {
	    Policy copy = new Policy();
	    Identified.copyTo(this, copy);
	    copy.availabilityEnd = availabilityEnd;
	    copy.availabilityLength = availabilityLength;
	    copy.availabilityStart = availabilityStart;
	    copy.availableCountries = Sets.newHashSet(availableCountries);
	    copy.drmPlayableFrom = drmPlayableFrom;
	    copy.price = price;
	    copy.revenueContract = revenueContract;
	    copy.platform = platform;
	    copy.network = network;
	    copy.actualAvailabilityStart = actualAvailabilityStart;
	    return copy;
	}
	
	public enum Platform {
		XBOX,
		PC,
		IOS,
		YOUVIEW, 
		TALK_TALK,
		BTVISION_CARDINAL,
		YOUVIEW_IPLAYER,
		YOUVIEW_ITVPLAYER,
		YOUVIEW_4OD,
		YOUVIEW_DEMAND5;
		
		public String key() {
			return name().toLowerCase();
		}
		    
		public static Platform fromKey(String key) {
			for (Platform platform: values()) {
				if (platform.key().equals(key)) {
					return platform;
				}
			}
			return null;
		}
	}
	
	public enum Network {
	    WIFI,
	    THREE_G;
	    
	    public String key() {
	        return name().toLowerCase();
	    }
	    
	    public static Network fromKey(String key) {
	        for (Network network : values()) {
	            if (network.key().equals(key)) {
	                return network;
	            }
	        }
	        return null;
	    }
	}
}
