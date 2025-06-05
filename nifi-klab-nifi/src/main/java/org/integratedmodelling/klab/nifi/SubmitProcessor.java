package org.integratedmodelling.klab.nifi;

import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.exception.ProcessException;

/**
 * Receive an externally resolved observation encoding the resolution data within its metadata.
 * Validate it and submit it to the configured scope, which must be a {@link
 * org.integratedmodelling.klab.api.scope.ContextScope} "tuned" to the appropriate geometry and
 * semantics that can receive the observation.
 */
public class SubmitProcessor extends AbstractProcessor {
  @Override
  public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
    var controller = getControllerServiceLookup().getControllerServiceIdentifiers(KlabControllerService.class);
  }
}
