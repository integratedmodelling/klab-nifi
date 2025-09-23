package org.integratedmodelling.klab.nifi.utils;

import kong.unirest.HttpResponse;
import kong.unirest.JsonNode;
import kong.unirest.Unirest;

/**
 * Use this class to submit an Observation to the NiFi ListenHTTP Processor.
 *
 * <p>Create the Observation payload using {@link KlabObservationNifiRequest} and submit it with
 * {@link #submitObservation(KlabObservationNifiRequest)}.
 */
public class KlabNifiListenHTTPClient {

  private final String host;
  private final String port;
  private final String healthPort;

  private KlabNifiListenHTTPClient(Builder builder) {
    this.host = builder.host;
    this.port = (builder.port != null) ? builder.port : "3306";
    this.healthPort = builder.healthPort;
  }

  /**
   * Submits an observation payload to the NiFi ListenHTTP Processor.
   *
   * @param req the observation payload
   * @throws Exception if the health check or post request fails
   */
  public void submitObservation(KlabObservationNifiRequest req) throws Exception {
    System.out.println("Submitting Observation to the NiFi ListenHTTP Processor");

    // Optional Health Check if set up in the ListenHTTP Processor in the Nifi Flow
    // If set in the library, then it would definitely perform a healthcheck call nonetheless
    if (healthPort != null) {
      HttpResponse<String> response =
          Unirest.get(buildUrl(healthPort, "/healthcheck"))
              .header("Accept", "application/json")
              .asString();

      if (!response.isSuccess()) {
        throw new KlabNifiException("NiFi ListenHTTP Processor not ready");
      }
      System.out.println("HealthCheck Passed");
    }

    // Send POST request with JSON body
    HttpResponse<JsonNode> postResponse =
        Unirest.post(buildUrl(port, ""))
            .header("Accept", "application/json")
            .header("Content-Type", "application/json")
            .body(req.toJson())
            .asJson();

    if (!postResponse.isSuccess()) {
      throw new KlabNifiException(
          String.format(
              "Failed to submit observation: HTTP %d - %s",
              postResponse.getStatus(),
              postResponse.getBody() != null
                  ? postResponse.getBody().toString()
                  : "No response body"));
    }

    System.out.println("Observation Payload Submitted Successfully");
  }

  private String buildUrl(String port, String path) {
    return String.format("%s:%s%s", host, port, path);
  }

  /** Builder for {@link KlabNifiListenHTTPClient}. */
  public static class Builder {
    private String host;
    private String port;
    private String healthPort;

    public Builder setHost(String host) {
      this.host = host;
      return this;
    }

    public Builder setPort(String port) {
      this.port = port;
      return this;
    }

    public Builder setHealthPort(String port) {
      this.healthPort = port;
      return this;
    }

    public KlabNifiListenHTTPClient build() throws KlabNifiException {
      if (this.host == null || this.port == null) {
        throw new KlabNifiException("Host or Port is null and is required for the client");
      }
      return new KlabNifiListenHTTPClient(this);
    }
  }
}
