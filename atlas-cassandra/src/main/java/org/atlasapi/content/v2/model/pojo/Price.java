package org.atlasapi.content.v2.model.pojo;

import java.util.Objects;

public class Price {

    private String currency;
    private Integer price;

    public String getCurrency() {
        return currency;
    }

    public void setCurrency(String currency) {
        this.currency = currency;
    }

    public Integer getPrice() {
        return price;
    }

    public void setPrice(Integer price) {
        this.price = price;
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) return true;
        if (object == null || getClass() != object.getClass()) return false;
        Price price1 = (Price) object;
        return Objects.equals(currency, price1.currency) &&
                Objects.equals(price, price1.price);
    }

    @Override
    public int hashCode() {
        return Objects.hash(currency, price);
    }
}
