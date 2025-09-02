package org.integratedmodelling.klab.nifi.utils;


import com.google.gson.Gson;

import java.util.Optional;

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
    this.projection = builder.projection.orElse("EPSG:4326");
    this.start = builder.start;
    this.end = builder.end;
    this.extension = builder.extension;
    this.unit = builder.unit;
    this.name = builder.name;
    this.urn = builder.urn;
    return this;
  }

  public String requestToJson() {
    return new Gson().toJson(this);
  }


  public static class Builder {
    private String shape;
    private String sgrid;
    private Optional<String> projection;
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
      this.projection = Optional.of(projection);
      return this;
    }

    public Builder setTime(long start, long end) {
      this.start = start;
      this.end = end;
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
