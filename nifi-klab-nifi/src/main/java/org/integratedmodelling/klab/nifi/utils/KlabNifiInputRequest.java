package org.integratedmodelling.klab.nifi.utils;


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

  public static class Builder {
    private String shape;
    private String sgrid;
    private Optional<String> projection;
    private long start;
    private long end;
    private double extension;
    private String unit;

    Builder setShape(String shape) {
      this.shape = shape;
      return this;
    }

    Builder setSgrid(String sgrid) {
      this.sgrid = sgrid;
      return this;
    }

    Builder setProjection(String projection) {
      this.projection = Optional.of(projection);
      return this;
    }

    Builder setTime(long start, long end) {
      this.start = start;
      this.end = end;
      return this;
    }

    Builder setScope(double extension, String unit) {
      this.extension = extension;
      this.unit = unit;
      return this;
    }

    private String name;
    private String urn;

    Builder setName(String name) {
      this.name = name;
      return this;
    }

    Builder setUrn(String urn) {
      this.urn = urn;
      return this;
    }
  }

  public static void main(String[] args) {
    var requestBuilder = new KlabNifiInputRequest.Builder()
        .setProjection("EPSG:4326")
        .setShape(
            "POLYGON((33.796 -7.086, 35.946 -7.086, 35.946 -9.41, 33.796 -9.41, 33.796 -7.086))")
        .setSgrid("1.km")
        .setTime(1325376000000L, 1356998400000L)
        .setScope(1.0, "year")
        .setName("testing")
            .setUrn("earth:Terrestrial earth:Region");
    var request = new KlabNifiInputRequest().build(requestBuilder);
  }
}
