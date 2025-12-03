package org.integratedmodelling.klab.nifi;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Set;

import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.integratedmodelling.common.utils.Utils;
import org.integratedmodelling.klab.api.collections.Parameters;
import org.integratedmodelling.klab.api.knowledge.KlabAsset;
import org.integratedmodelling.klab.api.scope.ContextScope;
import org.integratedmodelling.klab.api.scope.UserScope;
import org.integratedmodelling.klab.api.services.RuntimeService;


@Tags({"k.LAB", "WEED", "AI", "Semantic Web", "Digital Twins"})
@CapabilityDescription("Retrieves Asset from the Digital Twin with the Resolved Semantic Query"
        + "Should be placed after the Observation Relay Processor"
+ "Read the Attributes of the Resolved Observation from the Processor")

@SeeAlso({KlabControllerWithDTService.class, KlabObservationWithDT.class})

@ReadsAttributes({
        @ReadsAttribute(
                attribute = "observation.id",
                description = "Id of the Resolved Observation"
        ),
        @ReadsAttribute(
                attribute = "observation.type",
                description="Type (NUMBER / OBJECT etc.) of the Resolved Observation"
        ),
        @ReadsAttribute(
                attribute = "observation.urn",
                description = "URN of the Resolved Observation"
        ),
        @ReadsAttribute(
                attribute = "digital.twin.url",
                description = "URL of the Digital Twin to which the Observation" +
                        "is submitted"
        )
})
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)

public class KlabAssetRetriever extends AbstractProcessor {

    public static final PropertyDescriptor KLAB_CONTROLLER_SERVICE =
            new PropertyDescriptor.Builder()
                    .name("klab-controller-service")
                    .displayName("k.LAB Controller Service")
                    .description(
                            "The k.LAB Federation Controller Service for the User Scope at the Federation Level")
                    .required(true)
                    .identifiesControllerService(KlabController.class)
                    .build();

    public static final PropertyDescriptor NIFI_KLAB_OUTPUT_DIR =
            new PropertyDescriptor.Builder()
                    .name("klab-output-directory")
                    .displayName("Nifi K.LAB Output Directory")
                    .description(
                            "The Local Directory System, where to write the Results from the k.LAB Semantic Web")
                    .required(true)
                    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                    .build();

    public static final Relationship REL_FAILURE =
            new Relationship.Builder().description("Failed processing").name("failure").build();

    public static final Relationship REL_SUCCESS =
            new Relationship.Builder()
                    .name("success")
                    .description("Successfully generated FlowFiles")
                    .build();

    private Set<Relationship> relationships;
    private List<PropertyDescriptor> descriptors;
    private volatile KlabController klabController;
    private volatile UserScope userScope;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        descriptors = List.of(KLAB_CONTROLLER_SERVICE, NIFI_KLAB_OUTPUT_DIR);
        relationships = Set.of(REL_SUCCESS, REL_FAILURE);
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return this.descriptors;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        klabController =
                context.getProperty(KLAB_CONTROLLER_SERVICE).asControllerService(KlabController.class);
        userScope = (UserScope) klabController.getScope(UserScope.class);
        if (userScope == null) {
            getLogger()
                    .error("No UserScope available from the KlabController, Authentication failed possibly");
        }
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        FlowFile flowfile = session.get();
        getLogger().info("Getting the Required Attributes, and triggering a Asset Retriever process, " +
                "if it's a Quality Observation");

        String dtURL = flowfile.getAttribute("digital.twin.url");
        String obsType = flowfile.getAttribute("observation.type");
        String obsId = flowfile.getAttribute("observation.id");
        String obsUrn = flowfile.getAttribute("observation.urn");
        getLogger().info("Found the Observation URN: " + obsUrn);

        if (obsType.equals("NUMBER")) {
            ContextScope contextScope = (ContextScope) klabController.getScope(dtURL, ContextScope.class);
            if (contextScope == null) { // This should never happen, guaranteed with the Observation Relay Processor
                getLogger().info("No ContextScope available from the KlabController for the DT " + dtURL);
                contextScope = userScope.connect(Utils.URLs.newURL(dtURL));
                if (contextScope == null) {
                    getLogger().error("Unable to connect to the Digital Twin " + dtURL);
                    session.transfer(flowfile, REL_FAILURE);
                    return;
                }
            } else {
                getLogger().info("Found the Context Scope for the k.LAB Controller");
            }

            String nifiOutputDir = context.getProperty(NIFI_KLAB_OUTPUT_DIR).getValue();

            try {
                File mapImage = copyNifiOutput(
                        contextScope
                                .getService(RuntimeService.class)
                                .exportAsset(
                                        obsUrn,
                                        KlabAsset.KnowledgeClass.OBSERVATION,
                                        "image/png",
                                        Parameters.create("viewportX", 800, "viewportY", 800),
                                        contextScope),
                        "png", nifiOutputDir);

                getLogger().info("Exported the Required Observation to " + mapImage.getName());
                session.transfer(flowfile, REL_SUCCESS);

            } catch (Exception e) {
                getLogger().error("Exception while writing the output data to the system from klab");
                session.transfer(flowfile, REL_FAILURE);
            }
        } else {
            getLogger().info("Found the Observation Type to be: " + obsType
            + "Hence not exporting, only available for (QUALITY) NUMBER Observations");
            session.transfer(flowfile, REL_SUCCESS);
        }


    }


    public File copyNifiOutput(InputStream inputStream, String extension, String nifiOutputDir)
            throws IOException {

        File klabOutputDir = new File(nifiOutputDir);
        File tempFile = File.createTempFile("klab", extension.startsWith(".") ? extension : "." + extension,
                klabOutputDir);

        try (FileOutputStream out = new FileOutputStream(tempFile)) {
            byte[] buffer = new byte[1024];

            int len;
            while((len = inputStream.read(buffer)) != -1) {
                out.write(buffer, 0, len);
            }
        }

        return tempFile;
    }
}
