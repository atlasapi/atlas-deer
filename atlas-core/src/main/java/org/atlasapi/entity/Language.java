package org.atlasapi.entity;

import javax.annotation.Nullable;

public class Language {

    private String display;
    private String code;
    private String dubbing;

    public Language(){}

    public Language(String code, String display) {
        this.code = code;
        this.display = display;
    }

    @Nullable
    public String getCode() {
        return code;
    }

    public void setCode(@Nullable String code) {
        this.code = code;
    }

    @Nullable
    public String getDisplay() {
        return display;
    }

    public void setDisplay(@Nullable String display) {
        this.display = display;
    }

    @Nullable
    public String getDubbing() {
        return dubbing;
    }

    public void setDubbing(@Nullable String dubbing) {
        this.dubbing = dubbing;
    }
}