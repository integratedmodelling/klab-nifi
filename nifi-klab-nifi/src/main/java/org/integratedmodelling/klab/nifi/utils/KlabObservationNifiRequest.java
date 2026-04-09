package org.integratedmodelling.klab.nifi.utils;

import static org.integratedmodelling.klab.nifi.utils.KlabAttributes.KLAB_UNRESOLVED_OBS_ID;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import java.util.Date;
import java.util.Map;

import org.integratedmodelling.common.utils.Utils;
import org.integratedmodelling.klab.api.services.ResourcesService;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKTReader;

/**
 * Use this class to create an Observation payload, to send to the KlabObservation Nifi Processor,
 * or use the ListenHTTP Processor in order to convert the post payload, to an equivalent Nifi
 * Flowfile using {@link KlabNifiListenHTTPClient}
 */
public class KlabObservationNifiRequest {
  private final KlabContext context;
  private final String semantics;
  private final String digitalTwin;
  private final Map<String, Object> contextualizer;
  private final Map<String, String> metadata;
  private final long id;

  private KlabObservationNifiRequest(Builder builder) {
    this.context = builder.context;
    this.semantics = builder.semantics;
    this.digitalTwin = builder.digitalTwin;
    this.id = builder.id;
    this.contextualizer = builder.contextualizer;
    this.metadata = builder.metadata;
  }

  /** Serialize this object to JSON */
  public String toJson() {
    Gson gson = new GsonBuilder().setPrettyPrinting().create();
    return gson.toJson(this);
  }

  // getters
  public KlabContext getContext() {
    return context;
  }

  public Map<String, String> getMetadata() {return metadata;}

  public String getObservationSemantics() {
    return semantics;
  }

  public Map<String, Object> getContextualizer() {
    return contextualizer;
  }

  public String getDigitalTwin() { return digitalTwin; }

  public static class Builder {
    private KlabContext context;
    private Map<String, String> metadata;
    private String name;
    private String semantics;
    private long id = KLAB_UNRESOLVED_OBS_ID;
    private String digitalTwin;
    public boolean asContext;
    public Map<String, Object> contextualizer;
    public String namespace;

    public Builder setContextualizer(Map<String, Object> contextualizer) {
      this.contextualizer = contextualizer;
      return this;
    }

    public Builder setMetadata(Map<String, String> metadata) {
      this.metadata = metadata;
      return this;
    }

    public Builder setKlabContext(KlabContext ctx) {
      this.context = ctx;
      return this;
    }


    public Builder setObservationSemantics(String semantics) {
      this.semantics = semantics;
      return this;
    }

    public Builder setObservationId(long id) {
      this.id = id;
      return this;
    }

    public Builder setDigitalTwin(String digitalTwin) {
      this.digitalTwin = String.valueOf(Utils.URLs.newURL(digitalTwin));
      return this;
    }

    public KlabObservationNifiRequest build() throws KlabNifiException {
      if (this.name == null && this.asContext) {
        throw new KlabNifiException("Submitted Context Observation must have a Name");
      }

      if (this.semantics == null) {
        throw new KlabNifiException("Submitted Observation must have a Semantics");
      }

      if (this.digitalTwin == null) {
        throw new KlabNifiException("Submitted Observation Request must have a Digital Twin URL");
      }
      // A DT URL is not required in every case

      return new KlabObservationNifiRequest(this);
    }
  }

  public static class KlabContext {

    public static class Space {
      private final String shape;
      private final String sgrid;
      private final String proj;

      private Space(KlabContext.Space.Builder builder) {
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

        public KlabContext.Space.Builder setShape(String shape)
            throws KlabNifiException {
          if (!isValidWKT(shape)) {
            throw new KlabNifiException("Invalid WKT String");
          }
          this.shape = shape;
          return this;
        }

        public KlabContext.Space.Builder setShape(
            double minX, double minY, double maxX, double maxY) {
          this.shape =
              String.format(
                  "POLYGON((%f %f, %f %f, %f %f, %f %f, %f %f))",
                  minX, minY, maxX, minY, maxX, maxY, minX, maxY, minX, minY);
          return this;
        }

        public KlabContext.Space.Builder setGrid(String sgrid) {
          this.sgrid = sgrid;
          return this;
        }

        public KlabContext.Space.Builder setProj(String proj) {
          this.proj = proj;
          return this;
        }

        public KlabContext.Space build() throws KlabNifiException {
          if (this.shape == null) {
            throw new KlabNifiException("Shape cannot be null");
          }
          return new KlabContext.Space(this);
        }
      }
    }

    public static class Time {
      private final long tstart;
      private final long tend;
      private final String tunit;
      private final int tscope;

      private Time(KlabContext.Time.Builder builder) {
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
        private int tscope = 1; // default

        public KlabContext.Time.Builder setTime(Date start, Date end)
            throws KlabNifiException {
          if (start.after(end)) {
            throw new KlabNifiException("Start time can't be more than the end time");
          }
          this.tstart = start.toInstant().toEpochMilli();
          this.tend = end.toInstant().toEpochMilli();
          return this;
        }

        public KlabContext.Time.Builder setTime(long start, long end)
                throws KlabNifiException {
          if (start > end) {
            throw new KlabNifiException("Start time can't be more than the end time");
          }
          this.tstart = start;
          this.tend = end;
          return this;
        }

        public KlabContext.Time.Builder setTunit(String tunit) {
          this.tunit = tunit;
          return this;
        }

        public KlabContext.Time.Builder setTscope(int tscope) {
          this.tscope = tscope;
          return this;
        }

        public KlabContext.Time build() {
          return new KlabContext.Time(this);
        }
      }
    }

    private final Space space;
    private final Time time;
    private final String name;
    private final String namespace;

    private KlabContext(Builder builder) {
      this.space = builder.space;
      this.time = builder.time;
      this.namespace = builder.namespace;
      this.name = builder.name;
    }

    /** Getters */
    public Space getSpace() {
      return space;
    }

    public Time getTime() {
      return time;
    }

    public String getNamespace() {return namespace;}
    public String getName() {return name;}

    public static class Builder {
      private Space space;
      private Time time;
      private String name;
      private String namespace;

      public Builder setSpace(Space space) {
        this.space = space;
        return this;
      }

      public Builder setTime(Time time) {
        this.time = time;
        return this;
      }

      public Builder setName(String name) {
        this.name = name;
        return this;
      }

      public Builder setNamespace(String namespace) {
        this.namespace = namespace;
        return this;
      }

      public KlabContext build() {
        return new KlabContext(this);
      }
    }
  }

  /*
    This is the Resource Config for Persistant Resource, i.e. in the Contextualizer
    if the Persist is False, it's ignored altogether
   */
  public static class PersistantResourceConfig {
    public final String service;
    public final String catalog;
    public final String id;
    public final String namespace;
    public final  ResourcesService.SubmissionMode mode;

    public PersistantResourceConfig(Builder builder) {
      this.service = builder.service;
      this.catalog = builder.catalog;
      this.id = builder.id;
      this.namespace = builder.namespace;
      this.mode = builder.mode;
    }

    public static class Builder {
      private String service;
      private String catalog;
      private String id;
      private String namespace;
      private ResourcesService.SubmissionMode mode;

      public Builder setService(String service) {
        this.service = service;
        return this;
      }

      public Builder setCatalog(String catalog) {
        this.catalog = catalog;
        return this;
      }

      public Builder setId(String id) {
        this.id = id;
        return this;
      }

      public Builder setNamespace(String ns) {
        this.namespace = ns;
        return this;
      }

      public Builder setSubmissionMode(ResourcesService.SubmissionMode mode){
        this.mode = mode;
        return this;
      }

      public PersistantResourceConfig build() {
        return new PersistantResourceConfig(this);
      }
    }
  }



}
