package org.atlasapi.content;

import org.atlasapi.meta.annotations.FieldName;


public class Restriction extends Identified {

	private Boolean restricted = null;

	private Integer minimumAge = null;

	private String message = null;

	public void setRestricted(Boolean rated) {
		this.restricted = rated;
	}

	@FieldName("is_restricted")
	public Boolean isRestricted() {
		return restricted;
	}

	public void setMinimumAge(Integer minimumAge) {
		this.minimumAge = minimumAge;
	}

	@FieldName("minimum_age")
	public Integer getMinimumAge() {
		return minimumAge;
	}

	public void setMessage(String message) {
		this.message = message;
	}

	@FieldName("message")
	public String getMessage() {
		return message;
	}
	
	@FieldName("has_restriction_information")
	public boolean hasRestrictionInformation() {
	    return restricted != null;
	}

	public static Restriction from() {
		Restriction restriction = new Restriction();
		restriction.setRestricted(false);
		return restriction;
	}

	public static Restriction from(String message) {
		if (message == null) {
			return from();
		}
		Restriction restriction = new Restriction();
		restriction.setRestricted(true);
		restriction.setMessage(message);
		return restriction;
	}

	public static Restriction from(int minimumAge) {
		Restriction restriction = new Restriction();
		restriction.setRestricted(true);
		restriction.setMinimumAge(minimumAge);
		return restriction;
	}

	public static Restriction from(int minimumAge, String message) {
		Restriction restriction = new Restriction();
		restriction.setRestricted(true);
		restriction.setMinimumAge(minimumAge);
		restriction.setMessage(message);
		return restriction;
	}
	
	public Restriction copy() {
	    Restriction copy = new Restriction();
	    Identified.copyTo(this, copy);
	    copy.message = message;
	    copy.minimumAge = minimumAge;
	    copy.restricted = restricted;
	    return copy;
	}
}
