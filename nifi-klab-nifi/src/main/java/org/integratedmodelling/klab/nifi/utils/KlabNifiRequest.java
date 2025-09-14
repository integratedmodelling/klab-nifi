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
 * using {@link KlabNifiRequestHTTP}
 */
public class KlabNifiRequest {
    private final Geometry geometry;
    private final Observation observation;

    private KlabNifiRequest(Builder builder) {
        this.geometry = builder.geometry;
        this.observation = builder.observation;
    }

    public String toJson() {
        Gson gson = new GsonBuilder().setPrettyPrinting().create();
        return gson.toJson(this);
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

    public static class Geometry {
        private final Space space;
        private final Time time;

        private Geometry(Builder builder) { 
            this.space = builder.space; 
            this.time = builder.time; 
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

    public static class Space {
        private final String shape;
        private final String sgrid;
        private final String proj ;

        private Space(Builder builder) { 
            this.shape = builder.proj + builder.shape; 
            this.sgrid = builder.sgrid; 
            this.proj = builder.proj; 
        }

        /*
         * Validates if a WKT string is valid or not
         */
        public static boolean isValidWKT(String wkt) {
            WKTReader reader = new WKTReader();
            try {
                reader.read(wkt);  // Try parsing
                return true;       // No exception -> valid
            } catch (ParseException e) {
                return false;      // Exception -> invalid
            }
        }

        public static class Builder {
            private String shape;
            private String sgrid = "1.km";
            private String proj = "EPSG:4326";

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

        public static class Builder {
            private long tstart;
            private long tend;
            private String tunit = "year";
            private int tscope = 1;

            public Builder setTime(long start, long end) throws KlabNifiException {

                if (tstart > tend) {
                    throw new 
                    KlabNifiException("Start time can't be more than the end time");
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

    public static class Observation {
        private final String name;
        private final String semantics;

        private Observation(Builder builder) { 
            this.name = builder.name; 
            this.semantics = builder.semantics; 
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
                return this; }
            
            public Observation build() { 
                return new Observation(this); 
            }
        }
    }
}
