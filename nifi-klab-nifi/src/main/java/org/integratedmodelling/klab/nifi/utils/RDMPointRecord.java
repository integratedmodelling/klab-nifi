package org.integratedmodelling.klab.nifi.utils;

import org.apache.avro.generic.GenericData;
import org.apache.commons.text.StringEscapeUtils;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class RDMPointRecord {
    private final float lat;
    private final float lon;
    private final String Id;
    private final String orig_class;
    private final String orig_id;
    private final String description;
    private final Object timestamp;
    private final String eunis2021plus;
    private final String iucn_get;
    private final String eu;
    private final String type;


    private RDMPointRecord(Builder builder) {
        this.lat = builder.lat;
        this.lon = builder.lon;
        this.Id = builder.Id;
        this.orig_class = builder.orig_class;
        this.description = builder.description;
        this.timestamp = builder.timestamp;
        this.eunis2021plus = builder.eunis2021plus;
        this.iucn_get = builder.iucn_get;
        this.orig_id = builder.orig_id;
        this.eu = builder.eu;
        this.type = builder.type;
    }

    public float getLat() { return lat; }
    public float getLon() { return lon; }
    public String getOrigClass() { return orig_class; }
    public String getDescription() { return description; }
    public long getTimestamp() {
        if (timestamp instanceof GenericData.Fixed) {
            System.out.println("Timestamp found to be of Type: GenericData.Fixed");
            byte[] bytes = ((GenericData.Fixed) timestamp).bytes();
            ByteBuffer buffer = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN);

            long nanosOfDay = buffer.getLong(); // first 8 bytes
            int julianDay = buffer.getInt();    // last 4 bytes

            return (julianDay - 2440588L) * 86400000L
                    + (nanosOfDay / 1_000_000L);
        }
        System.out.println("Returning Long");
        return (long) timestamp;
    }

    public String getEunis2021plus() { return eunis2021plus; }
    public String getIUCNGet() { return iucn_get; }
    public String getEU() { return eu; }
    public String getId() {return Id;}
    public String getOrigId() {return orig_id;}
    public String getType() { return type; }


    // Builder
    public static class Builder {
        private float lat;
        private float lon;
        private String orig_class;
        private String description;
        private Object timestamp;
        private String eunis2021plus;
        private String iucn_get;
        private String eu;
        private String type;
        private String orig_id;
        private String Id;

        public Builder setLat(float lat) { this.lat = lat; return this; }
        public Builder setLon(float lon) { this.lon = lon; return this; }
        public Builder setOrigClass(String orig_class) { this.orig_class = orig_class; return this; }
        public Builder setDescription(String description) { this.description = description; return this; }
        public Builder setTimestamp(Object timestamp) { this.timestamp = timestamp; return this; }
        public Builder setEunis2021plus(String Eunis2021plus) { this.eunis2021plus = Eunis2021plus; return this; }
        public Builder setIUCNGet(String iucnGet) { this.iucn_get = iucnGet; return this; }
        public Builder setEU(String eu) { this.eu = eu; return this; }
        public Builder setType(String type) { this.type = type; return this; }
        public Builder setId(String id) {this.Id = id; return this; }
        public Builder setOrigId(String origId) {this.orig_id = origId; return this;}

        public RDMPointRecord build() {
            return new RDMPointRecord(this);
        }
    }
}