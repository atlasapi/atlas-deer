package org.atlasapi.content;

public enum TransportSubType {

    HTTP,
    RTSP,
    ITUNES,
    BRIGHTCOVE;

    public String toString() {
        return name().toLowerCase();
    }

    public static TransportSubType fromString(String s) {
        return valueOf(s.toUpperCase());
    }

    ;
}
