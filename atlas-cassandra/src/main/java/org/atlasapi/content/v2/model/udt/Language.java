package org.atlasapi.content.v2.model.udt;

import com.datastax.driver.mapping.annotations.Field;
import com.datastax.driver.mapping.annotations.UDT;

@UDT(name = "languageClass")
public class Language {

    @Field(name = "display") private String display;
    @Field(name = "code") private String code;
    @Field(name = "dubbing") private String dubbing;

    public String getDisplay() {
        return display;
    }

    public void setDisplay(String display) {
        this.display = display;
    }

    public String getCode() {
        return code;
    }

    public void setCode(String code) {
        this.code = code;
    }

    public String getDubbing() {
        return dubbing;
    }

    public void setDubbing(String dubbing) {
        this.dubbing = dubbing;
    }
}
