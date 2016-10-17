package org.atlasapi.entity;

import org.joda.time.DateTime;

public class Distribution {

    private String format;
    private DateTime releaseDate;
    private String distributor;

    private Distribution(Builder builder) {
        this.format = builder.format;
        this.releaseDate = builder.releaseDate;
        this.distributor = builder.distributor;
    }

    public static Builder builder() {
        return new Builder();
    }

    public String getFormat() {
        return format;
    }

    public void setFormat(String format) {
        this.format = format;
    }

    public DateTime getReleaseDate() {
        return releaseDate;
    }

    public void setReleaseDate(DateTime releaseDate) {
        this.releaseDate = releaseDate;
    }

    public String getDistributor() {
        return distributor;
    }

    public void setDistributor(String distributor) {
        this.distributor = distributor;
    }

    public static class Builder {
        private String format;
        private DateTime releaseDate;
        private String distributor;

        private Builder() {

        }

        public Builder withformat(String format) {
            this.format = format;
            return this;
        }

        public Builder withReleaseDate(DateTime releaseDate) {
            this.releaseDate = releaseDate;
            return this;
        }

        public Builder withDistributor(String distributor) {
            this.distributor = distributor;
            return this;
        }

        public Distribution build() {
            return new Distribution(this);
        }

    }

}
