package org.integratedmodelling.klab.nifi;


import com.google.gson.Gson;
import java.util.List;
import java.util.Set;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.integratedmodelling.klab.nifi.utils.KlabNifiException;
import org.integratedmodelling.klab.nifi.utils.KlabObservationNifiRequest;

/** An processor that builds a valid flowfile to be passed to the
 *  Observation Submitter Processor, by the means of setting different
 *  things in the Parameters */
@Tags({"k.LAB", "WEED", "AI", "Semantic Web", "Digital Twins"})
@CapabilityDescription("Generates FlowFiles when events are received from k.LAB Controller Service")
@InputRequirement(InputRequirement.Requirement.INPUT_FORBIDDEN)
public class KlabObservationRequestGenerator extends AbstractProcessor {

    public static final PropertyDescriptor DIGITAL_TWIN_URL_PROPERTY =
            new PropertyDescriptor.Builder()
                    .name("URL")
                    .displayName("Digital Twin URL")
                    .description("The URL for the digital twin to connect to")
                    .required(false)
                    .addValidator(StandardValidators.URL_VALIDATOR)
                    .build();

    public static final PropertyDescriptor OBSERVATION_NAME =
            new PropertyDescriptor.Builder()
                    .name("observation-name")
                    .displayName("Name of the observation context.")
                    .description("The name of the context observation, NEEDED if observing a context, not for quality observations and all.")
                    .required(false)
                    .build();

    public static final PropertyDescriptor OBSERVATION_SEMANTICS =
            new PropertyDescriptor.Builder()
                    .name("observation-semantics")
                    .displayName("Semantics of the observation.")
                    .description("These are the observed semantics.")
                    .required(true)
                    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                    .build();

    public static final PropertyDescriptor OBSERVATION_ID =
            new PropertyDescriptor.Builder()
                    .name("observation-id")
                    .displayName("ID of the observation.")
                    .description("These are the observed semantics. -1 for context observations.")
                    .required(false)
                    .addValidator(StandardValidators.NUMBER_VALIDATOR)
                    .defaultValue("-1")
                    .build();

    public static final PropertyDescriptor OBSERVATION_SPACE =
            new PropertyDescriptor.Builder()
                    .name("observation-space")
                    .displayName("Spatial dimension of the observation.")
                    .description("WKT geometry of the observation.")
                    .required(false)
                    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                    .build();

    public static final PropertyDescriptor OBSERVATION_PROJECTION =
            new PropertyDescriptor.Builder()
                    .name("observation-projection")
                    .displayName("Spatial projection of the observation.")
                    .description("Projection of the observation.")
                    .required(false)
                    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                    .defaultValue("EPSG:4326")
                    .build();

    public static final PropertyDescriptor OBSERVATION_GRID_SIZE =
            new PropertyDescriptor.Builder()
                    .name("observation-grid")
                    .displayName("Grid size of the observation.")
                    .description("Grid of the observation.")
                    .required(false)
                    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                    .defaultValue("1.km")
                    .build();

    public static final PropertyDescriptor OBSERVATION_TIME_START =
            new PropertyDescriptor.Builder()
                    .name("observation-tstart")
                    .displayName("Time start.")
                    .description("Starting time of observation as millis.")
                    .required(false)
                    .addValidator(StandardValidators.NUMBER_VALIDATOR)
                    .defaultValue("1577833200000")
                    .build();

    public static final PropertyDescriptor OBSERVATION_TIME_END =
            new PropertyDescriptor.Builder()
                    .name("observation-tend")
                    .displayName("Time end.")
                    .description("End time of the observation as millis.")
                    .required(false)
                    .addValidator(StandardValidators.NUMBER_VALIDATOR)
                    .defaultValue("1640991599000")
                    .build();

    public static final PropertyDescriptor OBSERVATION_TIME_UNIT =
            new PropertyDescriptor.Builder()
                    .name("observation-tunit")
                    .displayName("Temporal unit.")
                    .description("Temporal unit of the observation.")
                    .required(false)
                    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                    .defaultValue("year")
                    .build();

    public static final PropertyDescriptor OBSERVATION_TIME_SCOPE =
            new PropertyDescriptor.Builder()
                    .name("observation-tscope")
                    .displayName("Temporal scope.")
                    .description("Temporal scope of the observation.")
                    .required(false)
                    .addValidator(StandardValidators.NUMBER_VALIDATOR)
                    .defaultValue("1")
                    .build();

    public static final PropertyDescriptor AS_CONTEXT =
            new PropertyDescriptor.Builder()
                    .name("as_context")
                    .displayName("Set As Context")
                    .description("To consider this as a context observation, if true, time and space is required as well")
                    .required(false)
                    .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
                    .build();

    public static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS =
            List.of(OBSERVATION_TIME_SCOPE, DIGITAL_TWIN_URL_PROPERTY, OBSERVATION_NAME,
                    OBSERVATION_SEMANTICS, OBSERVATION_ID,
                    OBSERVATION_SPACE, OBSERVATION_PROJECTION, OBSERVATION_GRID_SIZE,
                    OBSERVATION_TIME_START, OBSERVATION_TIME_END, OBSERVATION_TIME_UNIT,
                    AS_CONTEXT);

    public static final Relationship REL_FAILURE =
            new Relationship.Builder().description("Failed processing").name("failure").build();

    public static final Relationship REL_SUCCESS =
            new Relationship.Builder()
                    .name("success")
                    .description("Successfully generated FlowFiles")
                    .build();

    private Set<Relationship> relationships;

    private static final Set<Relationship> RELATIONSHIPS = Set.of(REL_FAILURE, REL_SUCCESS);

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return Set.of(REL_FAILURE, REL_SUCCESS);
    }

    @Override
    protected void init(final ProcessorInitializationContext context) {
        relationships = Set.of(REL_FAILURE, REL_SUCCESS);
    }



    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {

        KlabObservationNifiRequest.Builder requestBuilder = new KlabObservationNifiRequest.Builder();
        try {
            var builder = new KlabObservationNifiRequest.Builder();

            String projection = context.getProperty(OBSERVATION_PROJECTION).getValue();
            String geometryWkt = context.getProperty(OBSERVATION_SPACE).getValue() == null // TODO if empty, ignore
                    || context.getProperty(OBSERVATION_SPACE).getValue().isBlank()
                    ? ""
                    : context.getProperty(OBSERVATION_SPACE).getValue();
            String grid = context.getProperty(OBSERVATION_GRID_SIZE).getValue();

            var space =
                    new KlabObservationNifiRequest.Geometry.Space.Builder()
                            .setProj(projection)
                            .setShape(geometryWkt)
                            .setGrid(grid)
                            .build();

            // 1325376000000L -> 1356998400000L
            long tStart = Long.parseLong(context.getProperty(OBSERVATION_TIME_START).getValue());
            long tEnd = Long.parseLong(context.getProperty(OBSERVATION_TIME_END).getValue());
            String tUnit = context.getProperty(OBSERVATION_TIME_UNIT).getValue();

            var time =
                    new KlabObservationNifiRequest.Geometry.Time.Builder()
                            .setTime(tStart, tEnd)
                            .setTscope(1)
                            .setTunit(tUnit)
                            .build();

            var geometry =
                    new KlabObservationNifiRequest.Geometry.Builder().setSpace(space).setTime(time).build();

            String name =
                    context.getProperty(OBSERVATION_NAME).getValue() == null
                            || context.getProperty(OBSERVATION_NAME).getValue().isBlank()
                            ? "testing-" + System.currentTimeMillis()
                            : context.getProperty(OBSERVATION_NAME).getValue();

            String semantics = context.getProperty(OBSERVATION_SEMANTICS).getValue();
            String dtURL = context.getProperty(DIGITAL_TWIN_URL_PROPERTY).getValue();
            boolean asContext = Boolean.parseBoolean(context.getProperty(AS_CONTEXT).getValue());

            FlowFile flowFile = session.create();

            KlabObservationNifiRequest request = requestBuilder
                    .setAsContext(asContext)
                    .setDigitalTwin(dtURL)
                    .setObservationSemantics(semantics)
                    .setObservationName(name)
                    .setGeometry(geometry)
                    .build();

            flowFile =
                    session.write(
                            flowFile,
                            out -> {
                                // Write event data to FlowFile content
                                out.write(new Gson().toJson(request).getBytes());
                            });

            // Add attributes from event
            session.transfer(flowFile, REL_SUCCESS);
            //session.commitAsync();

        } catch (Exception e) {
            getLogger().error("Failed to process event", e);
            session.rollback();
        }
    }
}
