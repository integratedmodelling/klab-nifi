# k.LAB NiFi Integration

Apache NiFi integration bundle for k.LAB, enabling k.LAB services and capabilities within NiFi data flows.

## Federated k.LAB environments

The k.LAB network can be used to build collaborative, distributed digital twins using the native k.LAB federation
capabilities. This package contains processors and service controllers used to enable k.LAB federation within NiFi
pipelines.

### Connecting to a federated hub

In order to establish a federated group, each user or service must belong to a k.LAB group that is tagged as a
federation. Currently, only one federation groups can be linked to a k.LAB certificate at any given time. The federation
will provide a broker URL and establish connection with the broker upon successful authentication, ensuring that all
federated scopes can communicate. Authentication and handshaking are performed by the k.LAB service controller, which
must be configured in each NiFi dataflow. The service controller holds a k.Lab user scope that can be accessed through
the controller's API.

If the dataflow includes a k.LAB controller, processors can be used to monitor messages, scopes, digital twins and the
observations in them, as well as any of the messages exchanged upon k.LAB events triggered at any federated location.
Events can be filtered and emitted so that the dataflow can react to them. Other processors can create k.LAB runtime
assets which will be communicated to any remote connected scopes.

## Requirements

- Apache NiFi 2.1.0
- A valid k.LAB certificate file (typically located at ~/.klab/klab.cert). Some functionalities are only available to
  k.LAB
  users who are part of a federated group.

## Building

The standard Maven build will leave the self-contained NAR file in nifi-klab-nifi-nar, ready for use within a NiFi
instance.

