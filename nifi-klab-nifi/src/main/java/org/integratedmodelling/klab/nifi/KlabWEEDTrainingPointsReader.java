package org.integratedmodelling.klab.nifi;
import com.google.gson.*;
import java.io.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.avro.AvroReadSupport;
import org.apache.parquet.conf.ParquetConfiguration;
import org.apache.parquet.io.LocalInputFile;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.Path;
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
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.SeekableInputStream;
import org.integratedmodelling.klab.nifi.utils.RDMPointRecord;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKBReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.integratedmodelling.klab.nifi.utils.KlabRDMTrainingPointRequest;

@Tags({"k.LAB", "WEED", "AI", "Semantic Web", "Digital Twins"})
@InputRequirement(
        InputRequirement.Requirement.INPUT_REQUIRED) // This shouldn't be the first processor

@CapabilityDescription(
            "Parses incoming Flowfiles from the RDM adhering to a particular schema and creates an Observation Obj"
                + "It would parse the GeoParquet file, and create the Observation Payload for submitting to the DT via the Nifi Proxy."
                + "The Processor that should follow it should be the KlabObservationWithDT Processor"
                + "Note that this Processor is not generic, and pertains to a particular flow in the WEED Project")


@SeeAlso( {KlabObservationWithDT.class})

public class KlabWEEDTrainingPointsReader extends AbstractProcessor {

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
    private List<PropertyDescriptor> descriptors;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        relationships = Set.of(REL_SUCCESS, REL_FAILURE);
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

        getLogger().info("Payload parsing done...");
        URL pqDownloadURL = req.get().getParquetDownloadURL();
        URL dtURL = req.get().getDTUrl();
        String collectionID = req.get().getCollectionId();
        int featureCount = req.get().getFeatureCount();

        getLogger().info("Found " + featureCount + "Number of points to analyse!");

        getLogger().info("Starting to Process the Parquet file");
        //https://iiasa.blob.core.windows.net/storage/coastal_2016_2020.parquet"

        try {
            Configuration conf = new Configuration();
            conf.setBoolean(AvroReadSupport.READ_INT96_AS_FIXED, true);

            File file = new File(downloadParquetToTemp(pqDownloadURL).getAbsolutePath());
            InputFile inputFile = new LocalInputFile(Paths.get(file.getAbsolutePath()));
            ParquetReader<GenericRecord> reader = AvroParquetReader
                    .<GenericRecord>builder(inputFile)
                    .withConf(conf)
                    .build();


            int count = 0;

            GenericRecord record;
            while ((record = reader.read()) != null && count < 10) {
                RDMPointRecord rdmPoint = getRDMPointFromGenericRecord(record);
               System.out.println("Lat: " + rdmPoint.getLat());
               System.out.println("Lon: " + rdmPoint.getLon());
                count += 1;
            }
            session.transfer(flowfile, REL_SUCCESS);
            reader.close();
        } catch (Exception e) {
            session.transfer(flowfile, REL_FAILURE);
            throw new RuntimeException(e);
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

    public static RDMPointRecord getRDMPointFromGenericRecord (GenericRecord record) {

        float pointLat = ((Number) record.get("lat")).floatValue();
        float pointLon = ((Number) record.get("lon")).floatValue();
        String origClass = record.get("orig_class") != null ? record.get("orig_class").toString() : null;
        String description = record.get("description") != null ? record.get("description").toString() : null;
        String timestamp = record.get("timestamp") != null ? record.get("timestamp").toString() : null;
        String eunis = record.get("eunis2021plus") != null ? record.get("eunis2021plus").toString() : null;
        String iucn = record.get("iucn_get") != null ? record.get("iucn_get").toString() : null;
        String eu = record.get("eu") != null ? record.get("eu").toString() : null;
        String type = record.get("type") != null ? record.get("type").toString() : null;

        return new RDMPointRecord.Builder()
                .setLat(pointLat)
                .setLon(pointLon)
                .setOrigClass(origClass)
                .setDescription(description)
                .setTimestamp(timestamp)
                .setEunis2021plus(eunis)
                .setIUCNGet(iucn)
                .setEU(eu)
                .setType(type)
                .build();


    }
}
