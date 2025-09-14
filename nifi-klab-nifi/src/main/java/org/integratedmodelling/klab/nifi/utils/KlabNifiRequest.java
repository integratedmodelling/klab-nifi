package org.integratedmodelling.klab.nifi.utils;

import java.util.Date;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.locationtech.jts.io.WKTReader;
import org.locationtech.jts.io.ParseException;

/**
 * Use this class to create an Observation payload, to
 * send to the KlabObservation Nifi Processor, or use the ListenHTTP
 * Processor in order to convert the post payload, to an equivalent Nifi Flowfile
 * using {@link KlabNifiListenHTTPClient}
 */
public class KlabNifiRequest {
    private final Geometry geometry;
    private final Observation observation;

    private KlabNifiRequest(Builder builder) {
        this.geometry = builder.geometry;
        this.observation = builder.observation;
    }

    /** Serialize this object to JSON */
    public String toJson() {
        Gson gson = new GsonBuilder().setPrettyPrinting().create();
        return gson.toJson(this);
    }

    // getters
    public Geometry getGeometry() {
        return geometry;
    }

    public Observation getObservation() {
        return observation;
    }

    public static class Builder {
        private Geometry geometry;
        private Observation observation;

        public Builder setGeometry(Geometry geometry) {
            this.geometry = geometry;
            return this;
        }

        public Builder setObservation(Observation observation) {
            this.observation = observation;
            return this;
        }

        public KlabNifiRequest build() {
            return new KlabNifiRequest(this);
        }
    }

    // -------------------- Geometry --------------------
    public static class Geometry {
        private final Space space;
        private final Time time;

        private Geometry(Builder builder) {
            this.space = builder.space;
            this.time = builder.time;
        }

        /** Getters */
        public Space getSpace() {
            return space;
        }

        public Time getTime() {
            return time;
        }

        public static class Builder {
            private Space space;
            private Time time;

            public Builder setSpace(Space space) {
                this.space = space;
                return this;
            }

            public Builder setTime(Time time) {
                this.time = time;
                return this;
            }

            public Geometry build() {
                return new Geometry(this);
            }
        }
    }

    // -------------------- Space --------------------
    public static class Space {
        private final String shape;
        private final String sgrid;
        private final String proj;

        private Space(Builder builder) {
            this.shape = builder.proj + " " + builder.shape;
            this.sgrid = builder.sgrid;
            this.proj = builder.proj;
        }

        /** Getters */
        public String getShape() {
            return shape;
        }

        public String getSgrid() {
            return sgrid;
        }

        public String getProj() {
            return proj;
        }

        /*
         * Validates if a WKT string is valid or not
         */
        public static boolean isValidWKT(String wkt) {
            WKTReader reader = new WKTReader();
            try {
                reader.read(wkt);
                return true;
            } catch (ParseException e) {
                return false;
            }
        }

        public static class Builder {
            private String shape;
            private String sgrid = "1.km"; // default
            private String proj = "EPSG:4326"; // default

            public Builder setShape(String shape) throws KlabNifiException {
                if (!isValidWKT(shape)) {
                    throw new KlabNifiException("Invalid WKT String");
                }
                this.shape = shape;
                return this;
            }

            public Builder setGrid(String sgrid) {
                this.sgrid = sgrid;
                return this;
            }

            public Builder setProj(String proj) {
                this.proj = proj;
                return this;
            }

            public Space build() {
                return new Space(this);
            }
        }
    }

    // -------------------- Time --------------------
    public static class Time {
        private final long tstart;
        private final long tend;
        private final String tunit;
        private final int tscope;

        private Time(Builder builder) {
            this.tstart = builder.tstart;
            this.tend = builder.tend;
            this.tunit = builder.tunit;
            this.tscope = builder.tscope;
        }

        /** Getters */
        public long getTstart() {
            return tstart;
        }

        public long getTend() {
            return tend;
        }

        public String getTunit() {
            return tunit;
        }

        public int getTscope() {
            return tscope;
        }

        public static class Builder {
            private long tstart;
            private long tend;
            private String tunit = "year"; // default
            private int tscope = 1;        // default

            public Builder setTime(long start, long end) throws KlabNifiException {
                if (start > end) {
                    throw new KlabNifiException("Start time can't be more than the end time");
                }
                this.tstart = start;
                this.tend = end;
                return this;
            }

            public Builder setTime(Date start, Date end) {
                this.tstart = start.toInstant().toEpochMilli();
                this.tend = end.toInstant().toEpochMilli();
                return this;
            }

            public Builder setTunit(String tunit) {
                this.tunit = tunit;
                return this;
            }

            public Builder setTscope(int tscope) {
                this.tscope = tscope;
                return this;
            }

            public Time build() {
                return new Time(this);
            }
        }
    }

    // -------------------- Observation --------------------
    public static class Observation {
        private final String name;
        private final String semantics;

        private Observation(Builder builder) {
            this.name = builder.name;
            this.semantics = builder.semantics;
        }

        /** Getters */
        public String getName() {
            return name;
        }

        public String getSemantics() {
            return semantics;
        }

        public static class Builder {
            private String name;
            private String semantics;

            public Builder setName(String name) {
                this.name = name;
                return this;
            }

            public Builder setSemantics(String semantics) {
                this.semantics = semantics;
                return this;
            }

            public Observation build() {
                return new Observation(this);
            }
        }
    }
}
