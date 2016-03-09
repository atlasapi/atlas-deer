package org.atlasapi.content.v2.model.udt;

import java.math.BigDecimal;

import com.datastax.driver.mapping.annotations.Field;
import com.datastax.driver.mapping.annotations.UDT;

@UDT(name = "price")
public class Price {

    @Field(name = "c")
    private String currency;

    @Field(name = "p")
    private BigDecimal price;

    public String getCurrency() {
        return currency;
    }

    public void setCurrency(String currency) {
        this.currency = currency;
    }

    public BigDecimal getPrice() {
        return price;
    }

    public void setPrice(BigDecimal price) {
        this.price = price;
    }
}
