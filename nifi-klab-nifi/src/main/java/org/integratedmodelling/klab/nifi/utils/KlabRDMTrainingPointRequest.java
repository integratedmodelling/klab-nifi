package org.integratedmodelling.klab.nifi.utils;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import java.net.URL;

public class KlabRDMTrainingPointRequest {
    private final String collectionId;
    private final int featureCount;
    private final URL dtURL;
    private final URL parquetDownloadUrl;

    private KlabRDMTrainingPointRequest(Builder builder) {
        this.collectionId  = builder.collectionId;
        this. featureCount = builder.featureCount;
        this.dtURL = builder.dtURL;
        this.parquetDownloadUrl = builder.parquetDownloadUrl;
    }

    /** Serialize this object to JSON */
    public String toJson() {
        Gson gson = new GsonBuilder().setPrettyPrinting().create();
        return gson.toJson(this);
    }

    // getters
    public String getCollectionId() {
        return collectionId;
    }

    public int getFeatureCount() {
        return featureCount;
    }

    public URL getParquetDownloadURL() {
        return parquetDownloadUrl;
    }

    public URL getDTUrl() { return dtURL; }

    public static class Builder {
        private String collectionId;
        private int featureCount;
        private URL dtURL;
        private URL parquetDownloadUrl;

        public Builder setCollectionId(String collectionId) {
            this.collectionId = collectionId;
            return this;
        }

        public Builder setfeatureCount(int featureCount) {
            this.featureCount = featureCount;
            return this;
        }


        public Builder setdtURL(URL dtURL) {
            this.dtURL = dtURL;
            return this;
        }

        public Builder setParquetDownloadURL(URL parquetURL) {
            this.parquetDownloadUrl = parquetURL;
            return this;
        }

        public KlabRDMTrainingPointRequest build() throws KlabNifiException {

            if (this.dtURL == null) {
                throw new KlabNifiException("Digital Twin URL is missing, Build failed!");
            }

            return new KlabRDMTrainingPointRequest(this);
        }
    }
}
