package org.integratedmodelling.klab.nifi.utils;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import java.net.URL;

public class KlabAsyncProcessingEmailNotifRequest {
    private final String recipient;
    private final String subject;
    private final String body;
    private final String dtURL;

    private KlabAsyncProcessingEmailNotifRequest(Builder builder) {
        this.recipient = builder.recipient;
        this.subject = builder.subject;
        this.body = builder.body;
        this.dtURL = builder.dtURL;
    }

    // getters
    public String getRecipient() {
        return recipient;
    }

    public String getSubject() {
        return subject;
    }

    public String getBody() { return body; }

    public String getDTURL() {return dtURL; }

    public static class Builder {
        private String recipient;
        private String subject;
        private String body;
        private String dtURL;

        public Builder setRecipient(String recipient) {
            this.recipient = recipient;
            return this;
        }

        public Builder setSubject(String subject) {
            this.subject = subject;
            return this;
        }


        public Builder setBody(String body) {
            this.body = body;
            return this;
        }

        public Builder setDTURL(String dtURL) {
            this.dtURL = dtURL;
            return this;
        }

        public KlabAsyncProcessingEmailNotifRequest build() throws KlabNifiException {

            if (this.recipient == null) {
                throw new KlabNifiException("Recipient Email is required, Build failed!");
            }

            return new KlabAsyncProcessingEmailNotifRequest(this);
        }
    }
}

