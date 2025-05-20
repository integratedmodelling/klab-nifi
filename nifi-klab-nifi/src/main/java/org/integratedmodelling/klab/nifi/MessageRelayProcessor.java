package org.integratedmodelling.klab.nifi;

import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.exception.ProcessException;

/**
 * Receive messages from the currently configured scope and relay them as output.
 *
 * <p>TODO add properties to filter messages
 */
public class MessageRelayProcessor extends AbstractProcessor {
  @Override
  public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {}
}
