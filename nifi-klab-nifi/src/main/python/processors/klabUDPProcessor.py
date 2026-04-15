from nifiapi.flowfiletransform import FlowFileTransform, FlowFileTransformResult
from nifiapi.documentation import use_case
from nifiapi.properties import PropertyDescriptor, StandardValidators
from nifiapi.relationship import Relationship

import openeo
import os
from time import sleep
import geopandas as gpd
from urllib.parse import quote
from shapely import to_geojson


@use_case("Triggers an UDP based on the Training Points Resolved by the Digital Twin")
class KlabWEEDTrainingPointsUDPStarter(FlowFileTransform):

    class Java:
        implements = ['org.apache.nifi.python.processor.FlowFileTransform']

    class ProcessorDetails:
        version = '0.0.1-SNAPSHOT'
        description = 'Triggers UDP based on the Training Points Resolved by the Digital Twin'
        tags = ["k.LAB", "WEED", "AI", "Semantic Web", "Digital Twins", "UDP", "OpenEO", "VITO"]


    SUCCESS = Relationship(name="success", description="Successfully Triggered the UDP and the UDP ran Successfully")
    FAILURE = Relationship(name="failure", description="Failed Processing the FlowFile or Triggering the UDP")

    def __init__(self, **kwargs):

        self.udp_namespace = PropertyDescriptor(
            name="UDP Namespace",
            description="Namespace of the UDP",
            validators=[StandardValidators.NON_EMPTY_VALIDATOR],
            required=True
        )

        self.udp_parameters = PropertyDescriptor(
            name="UDP Parameters",
            description="UDP Parameters comma separated",
            validators=[StandardValidators.NON_EMPTY_VALIDATOR],
            required=False
        )

        self.oidc_client_id = PropertyDescriptor(
            name="OIDC CLIENT ID",
            description="OIDC Client ID for authenticating with the OpenEO backend",
            validators=[StandardValidators.NON_EMPTY_VALIDATOR],
            required=True,
            sensitive=True # Senstive Field
        )

        self.oidc_client_secret = PropertyDescriptor(
            name="OIDC CLIENT SECRET",
            description="OIDC Client Secret for authenticating with the OpenEO backend",
            validators=[StandardValidators.NON_EMPTY_VALIDATOR],
            required=True,
            sensitive=True # Sensitive Field
        )

        self.descriptors = [self.udp_namespace, self.udp_parameters, self.oidc_client_id, self.oidc_client_secret]

    def getPropertyDescriptors(self):
        return self.descriptors

    def transform(self, context, flowfile):

        if flowfile is None:
            context.getLogger().error("Incoming flowfile is null")
            return FlowFileTransformResult(relationship="failure")

        dt_url = flowfile.getAttribute("rdm.points.dt.url")
        convex_hull = flowfile.getAttribute("rdm.points.convex.hull")

        if dt_url is None or dt_url.strip() == "" or convex_hull is None or convex_hull.strip() == "":
            context.getLogger().error("Missing attribute: rdm.points.dt.url or rdm.points.convex.hull")
            return FlowFileTransformResult(relationship="failure")


        udp_params_str = context.getProperty(self.udp_parameters).getValue()
        namespace = context.getProperty(self.udp_namespace).getValue()

        if udp_params_str is not None and udp_params_str.strip() != "":
            params = [param.strip() for param in udp_params_str.split(",")]
            param_dict = {}
            for param in params:   
                if "=" in param:
                    key, value = param.split("=", 1)
                    param_dict[key.strip()] = value.strip()
                else:
                    context.getLogger().warn(f"Invalid parameter format: {param}. Expected format is key=value.")
        
        connection = openeo.connect("openeo.dataspace.copernicus.eu").authenticate_oidc_client_credentials(
            client_id=context.getProperty(self.oidc_client_id).getValue(),
            client_secret=context.getProperty(self.oidc_client_secret).getValue()
        )


        cube = connection.datacube_from_process(
            process_id ="udp_trainstarter",
            namespace = namespace,
            geometry = convex_hull,
            year = param_dict.get("year", 2024),
            drm_table = param_dict.get("drm_table", "global-training"),
            digitalId = param_dict.get("digitalId", "B1"),
            scenarioId = param_dict.get("scenarioId", "AM1729"),
            dt_url = dt_url)
        
        job = cube.create_job(title=f'UDP_tests_{param_dict.get("digitalId")}_{param_dict.get("scenarioId")}_AOI', auto_add_save_result=False)
        job.start_job()

        print(job.job_id)
        
        while job.status() not in ['finished','error','canceled']:
            print(f"going to sleep job not yet done: status : {job.status()}")
            sleep(10)

        print(f"Job done: status : {job.status()}")
        if job.status() == 'finished':
            return FlowFileTransformResult(relationship="success")
        else:
            print (f"Job failed with status: {job.status()}")
            return FlowFileTransformResult(relationship="failure")
