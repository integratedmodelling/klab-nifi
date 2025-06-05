package org.integratedmodelling.klab.nifi;

import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.exception.ProcessException;

/**
 * Receive an unresolved observation and emit the correspondent contextualized one when the runtime
 * has produced it.
 */
public class ObserveProcessor extends AbstractProcessor {
  @Override
  public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
    var controller =
        getControllerServiceLookup().getControllerServiceIdentifiers(KlabControllerService.class);
  }
}
