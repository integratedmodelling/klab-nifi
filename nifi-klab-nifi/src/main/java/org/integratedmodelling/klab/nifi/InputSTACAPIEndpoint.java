package org.integratedmodelling.klab.nifi;

import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import java.util.List;
import java.util.Set;

/**
 * A Example Processor, a sort of Black Box which does some stuff.
 * In this case, it just takes an input, an Endpoint for STAC, and writes it as
 * an attribute to the Flowfile
 */


@Tags({"k.LAB", "WEED", "AI", "Semantic Web", "Digital Twins"})
@CapabilityDescription("This is First Test Processor for k.LAB 1.0 for Interacting with k.LAB Semantic Web, 1.0 through Digital Twins")
@SeeAlso({FileWriteProcessor.class, STACEndpointPingProcessor.class})
@WritesAttributes({@WritesAttribute(
        attribute="stac.url",
        description="Writes to Flowfile as an Attribute the STAC Endpoint input by User")})

public class InputSTACAPIEndpoint  extends AbstractProcessor {
    public static final PropertyDescriptor PROPERTY_STAC_ENDPOINT = new PropertyDescriptor
            .Builder()
            .name("STAC API Endpoint")
            .displayName("STAC API Endpoint URL")
            .description("STAC API Endpoint URL to connect to and get the data from")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor PROPERTY_CONTROLLER_SERVICE = new PropertyDescriptor
            .Builder()
            .name("k.LAB Controller Service")
            .displayName("k.LAB Controller Service")
            .description("Handle to k.LAB Digital Twin to Control, Manipulate and Update the Digital Twin in a Collaborative Fashion")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .identifiesControllerService(KlabControllerService.class)
            .build();


    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("On Success from the Processor")
            .build();

    private List<PropertyDescriptor> descriptors;

    private Set<Relationship> relationships;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        descriptors = List.of(PROPERTY_STAC_ENDPOINT, PROPERTY_CONTROLLER_SERVICE);

        relationships = Set.of(REL_SUCCESS);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }


    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.create(); // create a flow file..
        if (flowFile == null) {
            return;
        }
        String stacURL = context.getProperty(PROPERTY_STAC_ENDPOINT).getValue();
        KlabController controller = context.getProperty(PROPERTY_CONTROLLER_SERVICE).asControllerService(KlabController.class);

        getLogger().info("Writing Stuff to the Flowfile Attrbiute: stac.url " + stacURL);
        flowFile = session.putAttribute(flowFile, "stac.url", stacURL);
        session.transfer(flowFile, REL_SUCCESS);
    }
}


