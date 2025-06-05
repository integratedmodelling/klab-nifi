package org.integratedmodelling.klab.nifi;

import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.exception.ProcessException;

/**
 * Relay any new observations resolved in the configured scope. This is an emitter only.
 *
 * <p>TODO configure to filter for observables, scope, geometry etc.
 */
@Tags({"k.LAB", "Observations"})
@CapabilityDescription(
    "Relays any observations created by this user or others in the configured digital twin. Can be filtered as needed.")
public class ObservationRelayProcessor extends AbstractProcessor {

  @Override
  public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
    // Establish the output ('success') FlowFile and install a listener to pipe the observations
    // through it
    var controller = getControllerServiceLookup().getControllerServiceIdentifiers(KlabControllerService.class);

  }

  @OnScheduled
  public void initializeListeners() {}
}
