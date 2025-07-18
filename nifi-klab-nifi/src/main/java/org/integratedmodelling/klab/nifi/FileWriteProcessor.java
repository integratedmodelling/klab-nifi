package org.integratedmodelling.klab.nifi;

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;

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
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.integratedmodelling.klab.api.scope.UserScope;

import java.util.List;
import java.util.Set;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;

/**
 * A Example Processor, a sort of Black Box which does some stuff.
 * In this case, it just takes an input a Path to a Txt file, and appends a line to the txt file
 */

@Tags({"k.LAB", "WEED", "AI", "Semantic Web", "Digital Twins"})
@CapabilityDescription("This is Another Test Processor for k.LAB 1.0 for Interacting with k.LAB Semantic Web, 1.0 through Digital Twins")
@SeeAlso({STACEndpointPingProcessor.class, InputSTACAPIEndpoint.class})

@ReadsAttributes({
        @ReadsAttribute(attribute="stac.url",
                description="STAC URL pinged to"),
        @ReadsAttribute(attribute = "stac.result",
                description = "STAC Result i.e. the Result of Ping, to check if the Endpoint is valid or not")})

@WritesAttributes({@WritesAttribute(attribute="", description="")})

public class FileWriteProcessor extends AbstractProcessor {

    public static final PropertyDescriptor PROPERTY_FILE_LOC = new PropertyDescriptor
            .Builder()
            .name("STAC Results WriteFile Location")
            .displayName("STAC Results WriteFile Location")
            .description("Location of the Text File, where to write the result of STAC Testing")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .addValidator(StandardValidators.FILE_EXISTS_VALIDATOR)
            .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("On Success from the Processor")
            .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("On Failure from the Processor for some reason")
            .build();

    private List<PropertyDescriptor> descriptors;

    private Set<Relationship> relationships;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        descriptors = List.of(PROPERTY_FILE_LOC);

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
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        String fileToWrite = context.getProperty("STAC Results WriteFile Location").getValue();
        if (fileToWrite == null) {
            System.out.println("File to Write is found to be Null");
            session.transfer(flowFile, REL_FAILURE);
        } else {
            String stacURL = flowFile.getAttribute("stac.url");
            String stacURLPingResult = flowFile.getAttribute("stac.result");

            String lineToAppend = stacURL + " Was Pinged";

            if (stacURLPingResult.equals("true")) {
                lineToAppend += " Successfully :)";
            } else {
                lineToAppend += " Unsuccessfully :(";
            }

            try (BufferedWriter writer = new BufferedWriter(new FileWriter(fileToWrite, true))) {
                writer.write(lineToAppend);
                writer.newLine(); // adds a line break
                getLogger().info("Line appended successfully.");
                session.transfer(flowFile, REL_SUCCESS);
            } catch (IOException e) {
                getLogger().error("Error writing to file: " + e.getMessage());
                session.transfer(flowFile, REL_FAILURE);
            }
        }


    }
}

