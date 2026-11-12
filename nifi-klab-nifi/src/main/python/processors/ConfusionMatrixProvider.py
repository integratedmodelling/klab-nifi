"""Confusion-matrix providers. Convention: REFERENCE rows x PREDICTED columns."""
from pathlib import Path
import pandas as pd
from nifiapi.relationship import Relationship
from nifiapi.properties import PropertyDescriptor, StandardValidators
from nifiapi.flowfiletransform import FlowFileTransform, FlowFileTransformResult
import sys
from pyarrow import parquet
sys.path.insert(0, str(Path(__file__).resolve().parent))


class ConfusionMatrixProvider(FlowFileTransform):
    class Java:
        implements = ['org.apache.nifi.python.processor.FlowFileTransform']

    class ProcessorDetails:
        version = '0.0.1-SNAPSHOT'
        description = 'Confusion-matrix providers. Convention: REFERENCE rows x PREDICTED columns.'
        tags = ['confusion', 'matrix']
        dependencies = ['pandas', 'pyarrow']

    SUCCESS = Relationship(name='success', description='Successfully created confusion matrix')
    FAILURE = Relationship(name='failure', description='Failed to create confusion matrix')

    def __init__(self, **kwargs):
        self.bbox = PropertyDescriptor(
            name = "Bounding Box",
            description = "Bounding box as minX, minY, maxX, maxY, a GeoJSON object, or a path to a GeoJSON file",
            validators = [StandardValidators.NON_EMPTY_VALIDATOR],
            required=False
        )

        self.year = PropertyDescriptor(
            name="Reference Year",
            description="Calendar year (YYYY) used to select both ML and RB STAC items. "
                        "May be provided as a FlowFile attribute named year.",
            validators=[StandardValidators.NON_EMPTY_VALIDATOR],
            required=False
        )

        self.parquet_path = PropertyDescriptor(
            name="Validation Parquet Path",
            description="Path to the validation Parquet file containing reference points and metadata",
            validators=[StandardValidators.NON_EMPTY_VALIDATOR],
            required=False
        )

        self.mapping_csv = PropertyDescriptor(
            name="Class Mapping CSV",
            description="Path to the CSV file containing class mapping (EOTYPE, name, raster_value)",
            validators=[StandardValidators.NON_EMPTY_VALIDATOR],
            required=False
        )

        self.collections = PropertyDescriptor(
            name="STAC Collections",
            description="Comma-separated list of STAC collections to search for ML and RB items",
            validators=[StandardValidators.NON_EMPTY_VALIDATOR],
            required=False
        )

        self.descriptors = [self.bbox, self.year, self.parquet_path, self.mapping_csv, self.collections]

    def getPropertyDescriptors(self):
        return self.descriptors

    def getRelationships(self):
        return [self.SUCCESS, self.FAILURE]

    def read_mapping(path, eo_type='GET'):
        if not path:
            return {}
        df = pd.read_csv(path)
        for field in ('EOTYPE', 'name', 'raster_value'):
            if field not in df.columns:
                raise ValueError(f'Mapping is missing {field}')
        df = df[df.EOTYPE.astype(str).str.upper().str.strip() == eo_type.upper()]
        mapping = {}
        for _, row in df.iterrows():
            key = str(row['name']).strip()
            value = int(row['raster_value'])
            if key in mapping and mapping[key] != value:
                raise ValueError(f'Duplicate/conflicting class {key}')
            mapping[key] = value
        if eo_type.upper() == 'GET':
            import re
            for name, code in list(mapping.items()):
                match = re.match(r'^([A-Za-z][A-Za-z0-9]*(?:\.[0-9]+)*)\b', name)
                if match:
                    short = match.group(1)
                    if short in mapping and mapping[short] != code:
                        raise ValueError(f'Conflicting GET short code {short}')
                    mapping[short] = code
        return mapping

    def read_counts(path, fmt='csv', level=3, mapping=None):
        if fmt == 'csv':
            df = pd.read_csv(path, index_col=0)
            df.index = pd.Index([int(v) for v in df.index])
            df.columns = pd.Index([int(v) for v in df.columns])
        elif fmt == 'rdm-excel':
            mapping = mapping or {}
            if not mapping:
                raise ValueError('RDM Excel requires eo_maps.csv class mapping')
            raw = pd.read_excel(path, sheet_name=f'Level{level}Results', header=None)
            ref=[]; col=2
            while col < raw.shape[1] and isinstance(raw.iat[2,col],str) and raw.iat[2,col].strip() not in ('User Acc(%)',''):
                ref.append(raw.iat[2,col].strip()); col+=1
            if not ref:
                raise ValueError('No RDM reference classes found')
            predicted = [str(v).strip() for v in raw.iloc[3:3+len(ref),1]]
            data = raw.iloc[3:3+len(ref),2:2+len(ref)].apply(pd.to_numeric,errors='raise')
            df = data.T.copy()
            df.index=[mapping[v] for v in ref]
            df.columns=[mapping[v] for v in predicted]
        else:
            raise ValueError(f'Unknown matrix format: {fmt}')
        if not df.index.is_unique or not df.columns.is_unique:
            raise ValueError('Repeated class codes in confusion matrix')
        df = df.apply(pd.to_numeric,errors='raise')
        if df.isna().any().any() or (df < 0).any().any():
            raise ValueError('Invalid confusion matrix counts')
        return df

    def bbox_to_geojson(bbox_str):
        """Convert a bounding box string to a GeoJSON file."""
        import json
        from shapely.geometry import box, mapping
        coords = [float(x) for x in bbox_str.split(',')]
        if len(coords) != 4:
            raise ValueError('Bounding box must have 4 comma-separated values')
        geom = box(*coords)
        geojson = mapping(geom)
        return json.dumps(geojson)

    def from_validation_parquet(parquet_path, context_geojson, ml_tiles, rb_tiles, mapping_csv,
                                reference_column='iucn_get_code', eo_type='GET', level=3,
                                year_column=None, year=None, coverage_policy='both'):
        """Use the tested local spatial sampling functions (no RDM or QML)."""
        from contextlib import ExitStack
        from spatial_inputs import MosaicSource, read_context, read_validation_parquet, sample_validation, build_cms
        mapping = ConfusionMatrixProvider.read_mapping(mapping_csv, eo_type)
        if isinstance(context_geojson, str):
            if not context_geojson.strip().endswith('.geojson'):
                context_geojson = ConfusionMatrixProvider.bbox_to_geojson(context_geojson)
        geom = read_context(context_geojson, raster_paths=[Path(x) for x in ml_tiles+rb_tiles])
        with ExitStack() as stack:
            ml = stack.enter_context(MosaicSource([Path(x) for x in ml_tiles], geom, level=level))
            rb = stack.enter_context(MosaicSource([Path(x) for x in rb_tiles], geom, reference=ml))
            points, stats = read_validation_parquet(Path(parquet_path), geom, reference_col=reference_column,
                                                    year_col=year_column, year=year)
            if eo_type.upper() == 'GET':
                for name in ('iucn_get_code','iucn_get'):
                    if name not in points:
                        raise ValueError('Missing GET reference field: '+name)
                points = points.loc[points[['iucn_get_code','iucn_get']].notna().all(axis=1)].copy()
            ml_pairs, rb_pairs, sampling = sample_validation(points,ml,rb,mapping,reference_column,512,
                                                        require_both=coverage_policy=='both')
            cm_ml,cm_rb=build_cms(ml_pairs,rb_pairs,mapping)
            return cm_ml, cm_rb, {'validation':stats,'sampling':sampling}

    def transform(self, context, flowfile):
        self.logger.info("================= Starting ConfusionMatrixProvider =================")
        if flowfile is None:
            self.logger.error("Incoming flowfile is null")
            return FlowFileTransformResult(relationship="failure")

        #we check if the processor has built-in parameters or if they are provided as flowfile attributes
        bbox = context.getProperty(self.bbox).getValue()
        if bbox is None or not bbox.strip():
            bbox = flowfile.getAttribute("bbox")
        if bbox is None or not bbox.strip():
            self.logger.error("Bounding Box is missing from the processor property and FlowFile attribute 'bbox'")
            return FlowFileTransformResult(relationship="failure")

        year = context.getProperty(self.year).getValue()
        if year is None or not year.strip():
            year = flowfile.getAttribute("year")
        if year is None or not year.strip():
            self.logger.error("Reference Year is missing from the processor property and FlowFile attribute 'year'")
            return FlowFileTransformResult(relationship="failure")

        parquet_path = context.getProperty(self.parquet_path).getValue()
        if parquet_path is None or not parquet_path.strip():
            parquet_path = flowfile.getAttribute("parquet_path")
        if parquet_path is None or not parquet_path.strip():
            self.logger.error("Validation Parquet Path is missing from the processor property and FlowFile attribute 'parquet_path'")
            return FlowFileTransformResult(relationship="failure")

        mapping_csv = context.getProperty(self.mapping_csv).getValue()
        if mapping_csv is None or not mapping_csv.strip():
            mapping_csv = flowfile.getAttribute("mapping_csv")
        if mapping_csv is None or not mapping_csv.strip():
            self.logger.error("Class Mapping CSV is missing from the processor property and FlowFile attribute 'mapping_csv'")
            return FlowFileTransformResult(relationship="failure")

        collections = context.getProperty(self.collections).getValue()
        if collections is None or not collections.strip():
            collections = flowfile.getAttribute("collections")  
        if collections is None or not collections.strip():
            self.logger.error("STAC Collections is missing from the processor property and FlowFile attribute 'collections'")
            return FlowFileTransformResult(relationship="failure")
        
        try:

            root = Path("/opt/nifi/output")

            if not root.is_dir():
                raise ValueError(f"Output directory does not exist: {root}")

            tifs = [
                path for path in root.rglob("*")
                if path.is_file() and path.suffix.lower() in {".tif", ".tiff"}
            ]

            ml_tiles = [path for path in tifs if collections.split(',')[1] in str(path)]
            rb_tiles = [path for path in tifs if collections.split(',')[0] in str(path)]

            if not ml_tiles:
                raise ValueError(f"No tiles found for the specified collections: {collections.split(',')[1]}")
            elif not rb_tiles:
                raise ValueError(f"No tiles found for the specified collections: {collections.split(',')[0]}")
            
            cm_ml, cm_rb, stats = ConfusionMatrixProvider.from_validation_parquet(
                parquet_path=parquet_path,
                context_geojson=bbox,
                ml_tiles=ml_tiles,
                rb_tiles=rb_tiles,
                mapping_csv=mapping_csv
                )

            self.logger.info(f"Confusion Matrix ML:\n{cm_ml}")
            self.logger.info(f"Confusion Matrix RB:\n{cm_rb}")

            output_dir = Path("/opt/nifi/output/confusion_matrices")
            output_dir.mkdir(parents=True, exist_ok=True)
            ml_path = output_dir / "confusion_matrix_ml.csv"
            rb_path = output_dir / "confusion_matrix_rb.csv"
            cm_ml.to_csv(ml_path, index_label="reference")
            cm_rb.to_csv(rb_path, index_label="reference")
            self.logger.info(f"Saved ML confusion matrix to {ml_path}")
            self.logger.info(f"Saved RB confusion matrix to {rb_path}")

            return FlowFileTransformResult(
                relationship="success",
                attributes={
                    "ml_confusion_matrix_path": str(ml_path),
                    "rb_confusion_matrix_path": str(rb_path)
                }
            )

        except Exception as e:
            import traceback
            self.logger.error(f"An error occurred while creating confusion matrices: {e}")
            traceback.print_exc()
            return FlowFileTransformResult(
                relationship="failure",
                attributes={"error": str(e)}
            )
        
