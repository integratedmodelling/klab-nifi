package org.integratedmodelling.klab.nifi.utils;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import org.integratedmodelling.klab.api.knowledge.Observable;

import java.util.Date;

public class KlabNifiInputRequest {
  private String name;
  private String urn;
  private String shape;
  private String sgrid;
  private String projection;
  private long start;
  private long end;
  private double extension;
  private String unit;

  public KlabNifiInputRequest build(Builder builder) {
    this.shape = builder.shape;
    this.sgrid = builder.sgrid;
    this.projection = builder.projection.isEmpty() ? "EPSG:4326" : builder.projection;
    this.start = builder.start;
    this.end = builder.end;
    this.extension = builder.extension;
    this.unit = builder.unit;
    this.name = builder.name;
    this.urn = builder.urn;
    return this;
  }

  public String requestToJson(Observable observable) {
    var ret = new JsonObject();

    var observableJson = new Gson().toJsonTree(observable).getAsJsonObject();
    observableJson.addProperty("name", this.name);

    var geometryJson = getJsonObject();

    ret.add("observable", observableJson);
    ret.add("geometry", geometryJson);
    ret.addProperty("id", -1);
    return ret.toString();
  }

  private JsonObject getJsonObject() {
    var geometryJson = new JsonObject();
    var dimensionsJson = new JsonArray();
    var spaceJson = new JsonObject();
    var timeJson = new JsonObject();

    var spaceParamsJson = new JsonObject();
    spaceParamsJson.addProperty("shape", String.format("%s %s ", this.projection, this.shape));
    spaceParamsJson.addProperty("sgrid", this.sgrid);
    spaceParamsJson.addProperty("proj", this.projection);
    spaceJson.add("parameters", spaceParamsJson);

    var timeParamsJson = new JsonObject();
    timeParamsJson.addProperty("ttype", "PHYSICAL");
    timeParamsJson.addProperty("tstart", this.start);
    timeParamsJson.addProperty("tend", this.end);
    timeParamsJson.addProperty("tscope", this.extension);
    timeParamsJson.addProperty("tunit", this.unit);
    timeJson.add("parameters", timeParamsJson);

    dimensionsJson.add(spaceJson);
    dimensionsJson.add(timeJson);
    geometryJson.add("dimensions", dimensionsJson);
    return geometryJson;
  }

  public static class Builder {
    private String shape;
    private String sgrid;
    private String projection;
    private long start;
    private long end;
    private double extension;
    private String unit;

    public Builder setShape(String shape) {
      this.shape = shape;
      return this;
    }

    public Builder setSgrid(String sgrid) {
      this.sgrid = sgrid;
      return this;
    }

    public Builder setProjection(String projection) {
      this.projection = projection;
      return this;
    }

    public Builder setTime(long start, long end) {
      this.start = start;
      this.end = end;
      return this;
    }

    public Builder setTime(Date start, Date end) {
      this.start = start.toInstant().toEpochMilli();
      this.end = end.toInstant().toEpochMilli();
      return this;
    }

    public Builder setScope(double extension, String unit) {
      this.extension = extension;
      this.unit = unit;
      return this;
    }

    private String name;
    private String urn;

    public Builder setName(String name) {
      this.name = name;
      return this;
    }

    public Builder setUrn(String urn) {
      this.urn = urn;
      return this;
    }
  }
}
