package org.integratedmodelling.klab.nifi;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.util.StandardValidators;


import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import java.net.*;

@Tags({"k.LAB", "WEED", "AI", "Semantic Web", "Digital Twins"})
@CapabilityDescription("This Processor just makes a PING to the STAC Endpoint URL, and that's it!")

@SeeAlso({FileWriteProcessor.class, InputSTACAPIEndpoint.class})

@ReadsAttributes({@ReadsAttribute(attribute="stac.url",
        description="Reads the Attribute stac.url from the flowfile and pings")})

@WritesAttributes({@WritesAttribute(attribute="stac.url",
        description="Writes the stac.url to Flowfile Attribute"),

        @WritesAttribute(attribute = "stac.result",
                description = "Writes the result of the ping")
})

public class STACEndpointPingProcessor extends AbstractProcessor {
    //Relationships
    // Relation: On Success
    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("On Success from the Processor")
            .build();

    // Relation: On Failure
    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("On Failure for Some Reason")
            .build();

    private List<PropertyDescriptor> descriptors;

    private Set<Relationship> relationships;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        //descriptors = List.of(PROPERTY_STAC_ENDPOINT, PROPERTY_COG_BAND_NAME);

        relationships = Set.of(REL_SUCCESS, REL_FAILURE);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {

    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        Map<String, String> resultAttr = new HashMap<>() ;

        System.out.println("Hola.. received a flow file!");
        String propertySTACURL = flowFile.getAttribute("stac.url");
        resultAttr.put("stac.url", propertySTACURL);

        try {
            URL url = new URL(propertySTACURL);
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("GET");
            conn.setConnectTimeout(5000);
            conn.setReadTimeout(5000);
            int responseCode = conn.getResponseCode();
            if (responseCode != 200 && responseCode != 201) {
                System.out.println("Unable to connect to the STAC API...");
                resultAttr.put("stac.result", "false");
                flowFile = session.putAllAttributes(flowFile, resultAttr);
                session.transfer(flowFile, REL_FAILURE);
            } else {
                resultAttr.put("stac.result", "true");
                flowFile = session.putAllAttributes(flowFile, resultAttr);
                session.transfer(flowFile, REL_SUCCESS);
            }

        } catch (Exception e) {
            System.out.println("Exception!!!!!!!!");
            resultAttr.put("stac.result", "false");
            flowFile = session.putAllAttributes(flowFile, resultAttr);
            session.transfer(flowFile, REL_FAILURE);
            System.out.println("Failure on trigger");
            e.printStackTrace();
        }
    }
}


