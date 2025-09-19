package org.integratedmodelling.klab.nifi;

import static org.integratedmodelling.klab.nifi.utils.KlabAttributes.KLAB_UNRESOLVED_OBS_ID;

import com.google.gson.*;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.integratedmodelling.klab.api.digitaltwin.DigitalTwin;
import org.integratedmodelling.klab.api.geometry.impl.GeometryImpl;
import org.integratedmodelling.klab.api.knowledge.Observable;
import org.integratedmodelling.klab.api.knowledge.observation.Observation;
import org.integratedmodelling.klab.api.knowledge.observation.impl.ObservationImpl;
import org.integratedmodelling.klab.api.knowledge.observation.scale.time.Time;
import org.integratedmodelling.klab.api.scope.ContextScope;
import org.integratedmodelling.klab.api.services.Reasoner;
import org.integratedmodelling.klab.nifi.utils.KlabObservationNifiRequest;

@Tags({"k.LAB", "WEED", "AI", "Semantic Web", "Digital Twins"})
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED) // This shouldn't be the first processor

@CapabilityDescription( "Parses incoming Flowfiles and creates an Observation Obj"
        + "Also is responsible for some pre validation steps"
        + "Observation processor for the digital twin. Submitted unresolved observations "
        + "will be output as resolved; submitted resolved observations will be output as accepted, "
        + "or their already present observation will be output instead.")

@WritesAttributes(
        {
                @WritesAttribute(attribute="observation.id", description="Writes the Id of the Observation made"),
                @WritesAttribute(attribute = "observation.type", description = "Writes the type of the Observation made") })


public class KlabObservationWithDT {
}
