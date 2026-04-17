package org.integratedmodelling.klab.nifi;
import com.google.gson.*;
import java.io.*;

import org.apache.commons.text.StringEscapeUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.avro.AvroReadSupport;
import org.apache.parquet.io.LocalInputFile;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.avro.generic.GenericRecord;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
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
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.io.InputFile;
import org.integratedmodelling.klab.api.geometry.impl.GeometryImpl;
import org.integratedmodelling.klab.api.knowledge.Observable;
import org.integratedmodelling.klab.api.knowledge.Urn;
import org.integratedmodelling.klab.api.knowledge.observation.Observation;
import org.integratedmodelling.klab.api.knowledge.observation.scale.time.Time;
import org.integratedmodelling.klab.api.scope.ContextScope;
import org.integratedmodelling.klab.api.scope.UserScope;
import org.integratedmodelling.klab.api.services.Reasoner;
import org.integratedmodelling.klab.api.services.runtime.Message;
import org.integratedmodelling.klab.nifi.utils.RDMPointRecord;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.integratedmodelling.klab.nifi.utils.KlabRDMTrainingPointRequest;
import org.locationtech.jts.io.WKTReader;
import org.locationtech.jts.io.WKTWriter;

import static org.integratedmodelling.klab.nifi.utils.KlabAttributes.*;

@Tags({"k.LAB", "WEED", "AI", "Semantic Web", "Digital Twins"})
@InputRequirement(
        InputRequirement.Requirement.INPUT_REQUIRED) // This shouldn't be the first processor

@CapabilityDescription(
        "Parses incoming Flowfiles from the RDM adhering to a particular schema and creates an Observation Obj"
                + "It would parse the GeoParquet file, and create the Observation Payload for submitting to the DT via the Nifi Proxy."
                + "The Processor that should follow it should be the KlabObservationWithDT Processor"
                + "Note that this Processor is not generic, and pertains to a particular flow in the WEED Project")

@WritesAttributes({
        @WritesAttribute(attribute = "rdm.points.count", description = "Count of Points submitted to the Digital Twin"),
        @WritesAttribute(attribute = "rdm.points.convex.hull", description="Convex Hull of the Points Submitted"),
        @WritesAttribute(attribute="rdm.points.dt.url", description = "The Digital Twin URL to which the Points have been submitted")
})

@SeeAlso( {KlabObservationWithDT.class})

public class KlabWEEDTrainingPointsReader extends AbstractProcessor {

    public static final PropertyDescriptor KLAB_CONTROLLER_SERVICE =
            new PropertyDescriptor.Builder()
                    .name("klab-controller-service")
                    .displayName("k.LAB Controller Service")
                    .description(
                            "The k.LAB Federation Controller Service for the User Scope at the Federation Level")
                    .required(true)
                    .identifiesControllerService(KlabController.class)
                    .build();

    public static final Relationship REL_SUCCESS =
            new Relationship.Builder()
                    .name("success")
                    .description("Successfully Parsed the GeoParquet, and Created the Observation Payload")
                    .build();

    public static final Relationship REL_FAILURE =
            new Relationship.Builder()
                    .name("failure")
                    .description("GeoParquet Parsing failed")
                    .build();

    private Set<Relationship> relationships;
    private volatile KlabController klabController;
    private volatile UserScope userScope;
    private List<PropertyDescriptor> descriptors;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        descriptors = List.of(KLAB_CONTROLLER_SERVICE);
        relationships = Set.of(REL_SUCCESS, REL_FAILURE);
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
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {

        var pointsArray = new ArrayList<String>();
        FlowFile flowfile = session.get();
        if (flowfile == null) {
            getLogger().error("Incoming flowfile to the processor is null :( ");
            return;
        }

        getLogger().info("Found a flowfile, STarting to parse the flowfile...");

        final GsonBuilder builder = new GsonBuilder();
        AtomicReference<KlabRDMTrainingPointRequest> req = new AtomicReference<>();

        Gson gson = builder.create(); // Read JSON directly from FlowFile input stream
        session.read(
                flowfile,
                in -> {
                    try (InputStreamReader reader = new InputStreamReader(in, StandardCharsets.UTF_8)) {
                        req.set(gson.fromJson(reader, KlabRDMTrainingPointRequest.class));

                    } catch (Exception e) {
                        getLogger().error("Error reading JSON", e);
                    }
                });
        URL pqDownloadURL = req.get().getParquetDownloadURL();
        URL dtURL = req.get().getDTUrl();
        String collectionID = req.get().getCollectionId();
        int featureCount = req.get().getFeatureCount();

        getLogger().info("Found " + featureCount + "Number of points to add to Digital Twin " + dtURL.toString());
        getLogger().info("Starting to Process the Parquet file");
        ContextScope contextScope = (ContextScope) klabController.getScope(String.valueOf(dtURL), ContextScope.class);
        if (contextScope == null) {
            getLogger().info("No ContextScope available from the KlabController for the DT " + dtURL);
            contextScope = userScope.connect(dtURL);
            if (contextScope == null) {
                getLogger().error("Unable to connect to the Digital Twin " + dtURL);
                session.transfer(flowfile, REL_FAILURE);
                return;
            }
        } else {
            getLogger().info("Found the Context Scope for the k.LAB Controller");
        }
        Observable observable =
                contextScope
                        .getService(Reasoner.class)
                        .resolveObservable(KLAB_RDM_TRAINING_POINTS_OBSERVATION_SEMANTICS);
        Gson prettyGson = new GsonBuilder().setPrettyPrinting().create();

        try {
            Configuration conf = new Configuration();
            conf.setBoolean(AvroReadSupport.READ_INT96_AS_FIXED, true);
            conf.set("parquet.avro.readInt96AsTimestamp", "true");
            conf.set("parquet.avro.int96.timestamp.timezone", "UTC");

            File pqFile = new File(downloadParquetToTemp(pqDownloadURL).getAbsolutePath());
            InputFile inputFile = new LocalInputFile(Path.of(pqFile.getAbsolutePath()));
            ParquetReader<GenericRecord> reader = AvroParquetReader
                    .<GenericRecord>builder(inputFile)
                    .withConf(conf)
                    .build();


            int count = 0;

            GenericRecord record;
            while ((record = reader.read()) != null && count < featureCount) {
                RDMPointRecord rdmPoint = getRDMPointFromGenericRecord(record);
                var timeStamp = rdmPoint.getTimestamp();
                var geometry =
                        GeometryImpl.builder()
                                .space()
                                .shape(String.format("POINT (%f %f)", rdmPoint.getLon(), rdmPoint.getLat()))
                                .projection(KLAB_CONTEXT_PROJ)
                                .build()
                                .time()
                                .between(timeStamp, timeStamp)
                                .resolution(Time.Resolution.Type.YEAR, 1)
                                .build();

                var identity = Urn.of(KLAB_RDM_TRAINING_POINTS_NAMESPACE + ":" + collectionID + "-" + rdmPoint.getId()); // The Identity Problem
                getLogger().info("Received URN: " + identity.getUrn());
                var obs = contextScope.observation(observable)
                        .geometry(geometry.build())
                        .identity(KLAB_RDM_TRAINING_POINTS_NAMESPACE, collectionID+"-"+rdmPoint.getId())
                        .metadata(
                                Map.of(
                                        "iucn_get", rdmPoint.getIUCNGet(),
                                        "eunis2021plus", rdmPoint.getEunis2021plus(),
                                        "orig_id", rdmPoint.getOrigClass()
                                )
                        );
                Observation resolvedObs = KlabObservationWithDT.submitObservation(obs);
                if (resolvedObs == null) {
                    getLogger().error("Training Points Submission unsuccessful to the Digital Twin");
                    throw new Exception("Context Submission to DT: " + dtURL + "was Unsuccessful");
                } else {
                    System.out.println(prettyGson.toJson(resolvedObs));
                    ContextScope ctxS = contextScope.within(resolvedObs);
                    klabController.addScope(String.valueOf(dtURL), ctxS);
                    contextScope.send(
                            Message.MessageClass.DigitalTwin,
                            Message.MessageType.ContextObservationResolved,
                            resolvedObs);
                    getLogger().info("Context Observation was reolved successfully, the URN: " + resolvedObs.getUrn());
                    pointsArray.add(String.format("POINT (%f %f)", rdmPoint.getLon(), rdmPoint.getLat()));
                }
                count += 1;
            }
            reader.close();
            System.out.println(pointsArray.size() + " points have been successfully processed and added to the Digital Twin!");
            Map<String, String> attributes = new HashMap<>();
            attributes.put("rdm.points.count", String.valueOf(pointsArray.size()));
            attributes.put("rdm.points.collection.id", collectionID);
            attributes.put("rdm.points.convex.hull", convexHullWkt(pointsArray));
            attributes.put("rdm.points.dt.url", dtURL.toString());

            session.putAllAttributes(flowfile, attributes);
            getLogger().info("Deleting the Temp file: " + pqFile.getAbsolutePath());
            Files.delete(Path.of(pqFile.getAbsolutePath()));
            session.transfer(flowfile, REL_SUCCESS);
        } catch (Exception e) {
            getLogger().error("Processing failed", e);
            session.transfer(flowfile, REL_FAILURE);
        }
    }

    public static File downloadParquetToTemp(URL fileURL) throws IOException {
        // Create a temporary file (auto-deletes on JVM exit)
        File tempFile = File.createTempFile("geoparquet_", ".parquet");
        tempFile.deleteOnExit();

        HttpURLConnection connection = (HttpURLConnection) fileURL.openConnection();
        connection.setRequestMethod("GET");

        // Stream download into temp file
        try (InputStream in = connection.getInputStream();
             FileOutputStream out = new FileOutputStream(tempFile)) {

            byte[] buffer = new byte[8192];
            int bytesRead;
            while ((bytesRead = in.read(buffer)) != -1) {
                out.write(buffer, 0, bytesRead);
            }
        }

        System.out.println("Downloaded to temp file: " + tempFile.getAbsolutePath());
        return tempFile;  // Return the temp file object
    }

    public static String convexHullWkt(ArrayList<String> pointWkts) throws Exception {
        GeometryFactory gf = new GeometryFactory();
        WKTReader reader = new WKTReader(gf);

        List<Coordinate> coords = new ArrayList<>();
        for (String wkt : pointWkts) {
            Geometry g = reader.read(wkt);
            coords.add(g.getCoordinate());
        }

        Geometry geom = gf.createMultiPointFromCoords(coords.toArray(new Coordinate[0]));
        Geometry hull = geom.convexHull();
        System.out.println("Convex Hull Calculated for " + pointWkts.size() + " points");

        return new WKTWriter().write(hull);
    }

    public static RDMPointRecord getRDMPointFromGenericRecord (GenericRecord record) {

        float pointLat = ((Number) record.get("lat")).floatValue();
        float pointLon = ((Number) record.get("lon")).floatValue();
        String origClass = record.get("orig_class") != null ? StringEscapeUtils.unescapeJson(record.get("orig_class").toString()) : null;
        String origId = record.get("orig_id") != null ? StringEscapeUtils.unescapeJson(record.get("orig_id").toString()): null;
        String description = record.get("description") != null ? StringEscapeUtils.unescapeJson(record.get("description").toString()) : null;
        Object timestamp = record.get("timestamp") != null ?  record.get("timestamp") : null;
        String eunis = record.get("eunis2021plus") != null ? StringEscapeUtils.unescapeJson(record.get("eunis2021plus").toString()) : null;
        String iucn = record.get("iucn_get") != null ? StringEscapeUtils.unescapeJson(record.get("iucn_get").toString()) : null;
        String eu = record.get("eu") != null ? StringEscapeUtils.unescapeJson(record.get("eu").toString()) : null;
        String type = record.get("type") != null ? StringEscapeUtils.unescapeJson(record.get("type").toString()) : null;
        String id = record.get("Id") != null ? StringEscapeUtils.unescapeJson(record.get("Id").toString()) : null;

        return new RDMPointRecord.Builder()
                .setLat(pointLat)
                .setLon(pointLon)
                .setOrigClass(origClass)
                .setDescription(description)
                .setOrigId(origId)
                .setTimestamp(timestamp)
                .setEunis2021plus(eunis)
                .setIUCNGet(iucn)
                .setEU(eu)
                .setId(id)
                .setType(type)
                .build();
    }
}
