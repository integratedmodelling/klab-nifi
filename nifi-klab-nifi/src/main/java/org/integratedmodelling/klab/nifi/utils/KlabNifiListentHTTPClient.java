package org.integratedmodelling.klab.nifi.utils;

import kong.unirest.HttpResponse;
import kong.unirest.JsonNode;
import kong.unirest.Unirest;

/**
 * Use this class, to Submit an Observation, to the Nifi ListenHTTP Processor
 * Using the {@link KlabNifiRequest} Class to create the Observation payload, and 
 * submit it using the {@link #SubmitObservation} Method
 */
public class KlabNifiListentHTTPClient {

    private String host;
    private String port = "3306";
    private String healthPort = null;


    public KlabNifiListentHTTPClient (Builder builder){
        this.host = builder.host;
        this.port = builder.port;
        this.healthPort = builder.healthPort;
    }

    public void SubmitObservation(KlabNifiRequest req) throws Exception{
        System.out.println("Submitting Observation to the Nifi ListenHTTP Processor");
        if (healthPort != null) {
            System.out.println("Checking HealthCheck");

            HttpResponse<JsonNode> response = Unirest.
            get(host + ":" + healthPort + "/healthcheck")
                .header("Accept", "application/json")
                .asJson();

            if (!response.isSuccess()) {
                throw new KlabNifiException("Nifi ListenHTTP Processor not ready");
            }
        }


        HttpResponse<JsonNode> postResponse = Unirest.
        post(host + ":" +port)
            .header("Accept", "application/json")
            .header("Content-Type", "application/json")
            .body(req.toJson())
            .asJson();

        if (!postResponse.isSuccess()) {
            throw new KlabNifiException(
                "Failed to submit observation: HTTP " + 
                postResponse.getStatus() +
                " - " + postResponse.getBody()
            );
        } 

        System.out.println("Observation Payload Submitted Successfully");
    }


    public static class Builder {
        private String host;
        private String port;
        private String healthPort;

        public Builder setHost(String host){
            this.host = host;
            return this;
        }

        public Builder setPort(String port){
            this.port = port;
            return this;
        }

        public Builder setHealthport(String port){
            this.healthPort = port;
            return this;
        } 
    }

}
