package org.integratedmodelling.klab.nifi.utils;

import kong.unirest.HttpResponse;
import kong.unirest.JsonNode;
import kong.unirest.Unirest;

public class KlabNifiRequestHTTP {

    private String host;
    private String port;
    private String healthPort;


    public KlabNifiRequestHTTP build(Builder builder){
        this.host = builder.host;
        this.port = builder.port;
        this.healthPort = builder.healthport;
        return this;
    }


    public static class Builder {
        private String host;
        private String port;
        private String healthport;

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
