package org.integratedmodelling.klab.nifi.utils;

public class RDMPointRecord {
    private final float lat;
    private final float lon;
    private final String orig_class;
    private final String description;
    private final String timestamp;
    private final String eunis2021plus;
    private final String iucn_get;
    private final String eu;
    private final String type;


    private RDMPointRecord(Builder builder) {
        this.lat = builder.lat;
        this.lon = builder.lon;
        this.orig_class = builder.orig_class;
        this.description = builder.description;
        this.timestamp = builder.timestamp;
        this.eunis2021plus = builder.eunis2021plus;
        this.iucn_get = builder.iucn_get;
        this.eu = builder.eu;
        this.type = builder.type;
    }

    public float getLat() { return lat; }
    public float getLon() { return lon; }
    public String getOrigClass() { return orig_class; }
    public String getDescription() { return description; }
    public String getTimestamp() { return timestamp; }
    public String getEunis2021plus() { return eunis2021plus; }
    public String getIUCNGet() { return iucn_get; }
    public String getEU() { return eu; }
    public String getType() { return type; }


    // Builder
    public static class Builder {
        private float lat;
        private float lon;
        private String orig_class;
        private String description;
        private String timestamp;
        private String eunis2021plus;
        private String iucn_get;
        private String eu;
        private String type;

        public Builder setLat(float lat) { this.lat = lat; return this; }
        public Builder setLon(float lon) { this.lon = lon; return this; }
        public Builder setOrigClass(String orig_class) { this.orig_class = orig_class; return this; }
        public Builder setDescription(String description) { this.description = description; return this; }
        public Builder setTimestamp(String timestamp) { this.timestamp = timestamp; return this; }
        public Builder setEunis2021plus(String Eunis2021plus) { this.eunis2021plus = Eunis2021plus; return this; }
        public Builder setIUCNGet(String iucnGet) { this.iucn_get = iucnGet; return this; }
        public Builder setEU(String eu) { this.eu = eu; return this; }
        public Builder setType(String type) { this.type = type; return this; }

        public RDMPointRecord build() {
            return new RDMPointRecord(this);
        }
    }
}