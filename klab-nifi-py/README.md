## Klab Nifi Observation Relay Workflow


This is a Python Workflow, which can be used to create an Observation Payload, and submit it to the Observation Relay Processor, possibly via the HTTP Listener Port, or via custom workflows incorportating this. 

Note that, Custom Nifi Processors can be generated using Python as well, or can be a Docker based Processor as well or maybe made using ExecuteScript Processor as well. 

If, the Listen HTTP Processor is used exclusively, and the Observation can be added using a Post Call with the Generated JSON Payload.