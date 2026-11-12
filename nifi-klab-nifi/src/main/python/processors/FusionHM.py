"""WEED hybrid fusion V2: row-normalized CMs, uniform empty reference rows.

Confusion matrices: rows=reference class, columns=predicted class; raw counts.
Both input rasters: aligned single-band categorical GeoTIFFs with declared NoData.
"""
from __future__ import annotations
from collections import Counter
from contextlib import ExitStack, nullcontext
import json
import csv
from pathlib import Path
import numpy as np
import pandas as pd
import rasterio
from rasterio.windows import Window
from nifiapi.relationship import Relationship
from nifiapi.properties import PropertyDescriptor, StandardValidators
from nifiapi.flowfiletransform import FlowFileTransform, FlowFileTransformResult
#we add the path to the current directory to the sys.path so that we can import the spatial_inputs module
import sys
sys.path.insert(0, str(Path(__file__).resolve().parent))


class FusionHM(FlowFileTransform):
    class Java:
        implements = ['org.apache.nifi.python.processor.FlowFileTransform']

    class ProcessorDetails:
        version = '0.0.1-SNAPSHOT'
        description = 'WEED hybrid fusion V2: row-normalized CMs, uniform empty reference rows'
        tags = ['WEED', 'fusion', 'hybrid', 'classification', 'Hybrid', 'Modelling']
        dependencies = ['rasterio', 'numpy', 'pandas']

    SUCESS = Relationship(name='success', description='Successfully created HM output')
    FAILURE = Relationship(name='failure', description='Failed to create HM output')

    def __init__(self, **kwargs):

        self.collections = PropertyDescriptor(
            name='STAC Collections',
            description="Comma separated STAC collection IDs in RB, ML order, matching ConfusionMatrixProvider.",
            validators=[StandardValidators.NON_EMPTY_VALIDATOR],
            required=False
        )

        self.ml_confusion_matrix_path = PropertyDescriptor(
            name='ML Confusion Matrix Path',
            description="Path to ML confusion matrix CSV file.",
            validators=[StandardValidators.NON_EMPTY_VALIDATOR],
            required=False
        )

        self.rb_confusion_matrix_path = PropertyDescriptor(
            name='RB Confusion Matrix Path',
            description="Path to RB confusion matrix CSV file.",
            validators=[StandardValidators.NON_EMPTY_VALIDATOR],
            required=False
        )

        self.descriptors = [self.collections, self.ml_confusion_matrix_path, self.rb_confusion_matrix_path]

    def getPropertyDescriptors(self):
        return self.descriptors     

    def getRelationships(self):
        return [self.SUCCESS, self.FAILURE]

    def _blocks(width, height, block_size):
        for row in range(0, height, block_size):
            for col in range(0, width, block_size):
                yield Window(col, row, min(block_size, width-col), min(block_size, height-row))


    def _read(ds, window):
        data = ds.read(1, window=window, masked=True)
        valid = ~np.ma.getmaskarray(data)
        raw = np.asarray(data.data)
        if np.issubdtype(raw.dtype, np.floating):
            valid &= np.isfinite(raw)
        values = raw[valid]
        if values.size and (np.any(values <= 0) or np.any(values > 2147483647) or
                            np.any(values != np.floor(values))):
            raise ValueError('Valid raster pixels must have positive Int32 class codes')
        result = np.zeros(raw.shape, dtype='int32')
        result[valid] = raw[valid].astype('int32')
        return result, valid


    def _validate_cm(matrix, label):
        if not isinstance(matrix, pd.DataFrame) or matrix.empty:
            raise ValueError(f'{label}: expected nonempty pandas count matrix')
        matrix = matrix.copy()
        matrix.index = pd.Index([int(x) for x in matrix.index])
        matrix.columns = pd.Index([int(x) for x in matrix.columns])
        if not matrix.index.is_unique or not matrix.columns.is_unique:
            raise ValueError(f'{label}: duplicate numeric class codes')
        matrix = matrix.apply(pd.to_numeric, errors='raise')
        arr = matrix.to_numpy(dtype='float64')
        if not np.all(np.isfinite(arr)) or np.any(arr < 0):
            raise ValueError(f'{label}: invalid counts')
        return matrix.astype('float64')


    def _counts(src_ml, src_rb, block_size, coverage_policy):
        counts_ml, counts_rb, combinations = Counter(), Counter(), Counter()
        for window in FusionHM._blocks(src_ml.width, src_ml.height, block_size):
            a, valid_a = FusionHM._read(src_ml, window)
            b, valid_b = FusionHM._read(src_rb, window)
            active_a = (valid_a & valid_b) if coverage_policy == 'both' else valid_a
            active_b = (valid_a & valid_b) if coverage_policy == 'both' else valid_b
            for code, amount in zip(*np.unique(a[active_a], return_counts=True)):
                counts_ml[int(code)] += int(amount)
            for code, amount in zip(*np.unique(b[active_b], return_counts=True)):
                counts_rb[int(code)] += int(amount)
            both = valid_a & valid_b
            if both.any():
                pairs, amounts = np.unique(np.column_stack((a[both], b[both])), axis=0, return_counts=True)
                for (x, y), amount in zip(pairs, amounts):
                    combinations[(int(x), int(y))] += int(amount)
        return counts_ml, counts_rb, combinations


    def _validate_rasters(ml, rb):
        if ml.count != 1 or rb.count != 1:
            raise ValueError('V2 requires single-band categorical ML/RB rasters (not probability cubes)')
        if ml.nodata is None or rb.nodata is None or ml.crs is None or rb.crs is None:
            raise ValueError('Both rasters require declared CRS and NoData')
        if ml.width != rb.width or ml.height != rb.height or ml.crs != rb.crs or not ml.transform.almost_equals(rb.transform):
            raise ValueError('ML/RB rasters must be on precisely the same pixel grid; align before fusion')


    def fuse_v2(ml_path, rb_path, ml_cm, rb_cm, output_dir, block_size=512,
                coverage_policy='both', write_confidence=False, overwrite=False,
                top3_layout='none', global_stats=None):
        """Return paths and diagnostics. No network, no changes to inputs.

        'both': only shared valid pixels; 'either': ML/RB-only fallback as in NiFi.
        For both: empirical prior over common-valid domain (same as tilewise test).
        For either: original NiFi prior over full map rectangle, including NoData.
        """
        if top3_layout not in ('none', 'rasters', 'index', 'both'):
            raise ValueError('top3_layout must be none, rasters, index or both')
        if coverage_policy not in ('both', 'either'):
            raise ValueError('coverage_policy must be both or either')
        if not 16 <= block_size <= 8192:
            raise ValueError('block_size must be 16..8192')
        ml_cm = FusionHM._validate_cm(ml_cm, 'ML'); rb_cm = FusionHM._validate_cm(rb_cm, 'RB')

        output_dir = Path(output_dir / 'HM')
        output_dir.mkdir(parents=True, exist_ok=True)
        targets = [output_dir/'hybridMap_classification.tif', output_dir/'classification_method.tif', output_dir/'fusion_v2_summary.json']
        if write_confidence:
            targets.append(output_dir/'hybridMap_confidence.tif')
        top3_names = ('top1_probability.tif', 'top2_classification.tif', 'top2_probability.tif',
                    'top3_classification.tif', 'top3_probability.tif')
        if top3_layout in ('rasters', 'both'):
            targets.extend(output_dir / name for name in top3_names)
        if top3_layout in ('index', 'both'):
            targets.extend((output_dir/'prediction_pair_index.tif', output_dir/'prediction_pair_lookup.csv'))
        if not overwrite and any(path.exists() for path in targets):
            raise FileExistsError('One or more output files already exist; use overwrite=True')
        ml_context = nullcontext(ml_path) if hasattr(ml_path, 'read') else rasterio.open(ml_path)
        rb_context = nullcontext(rb_path) if hasattr(rb_path, 'read') else rasterio.open(rb_path)
        with ml_context as ml, rb_context as rb:
            FusionHM._validate_rasters(ml, rb)
            ml_counts, rb_counts, pairs = FusionHM._counts(ml, rb, block_size, coverage_policy)
            # Optional context-wide statistics retain one prior and vocabulary across all tiles.
            prior_ml = ml_counts if global_stats is None else Counter({int(k): int(v) for k,v in global_stats['ml_counts'].items()})
            prior_rb = rb_counts if global_stats is None else Counter({int(k): int(v) for k,v in global_stats['rb_counts'].items()})
            classes = sorted(set(ml_cm.index) | set(ml_cm.columns) | set(rb_cm.index) |
                            set(rb_cm.columns) | set(prior_ml) | set(prior_rb))
            if not classes or not pairs:
                raise ValueError('No common valid ML/RB pixels to fuse')
            index = {code: i for i, code in enumerate(classes)}
            k = len(classes)
            prior_pixels = (sum(pairs.values()) if coverage_policy == 'both' else ml.width*ml.height) if global_stats is None else int(global_stats['prior_pixels'])
            if prior_pixels <= 0:
                raise ValueError('Global prior denominator must be positive')
            prior = np.array([(prior_ml[c] + prior_rb[c])/(2*prior_pixels) for c in classes], dtype='float64')
            likelihoods = []
            empty_rows = []
            for matrix in (ml_cm, rb_cm):
                table = matrix.reindex(index=classes, columns=classes, fill_value=0).to_numpy(dtype='float64')
                totals = table.sum(axis=1)
                missing = totals == 0
                probabilities = np.divide(table, totals[:, None], out=np.zeros_like(table), where=totals[:, None] != 0)
                probabilities[missing, :] = 1.0/k
                likelihoods.append(probabilities)
                empty_rows.append([classes[i] for i in np.flatnonzero(missing)])
            choices = {}
            ranks = {}
            for (a, b) in sorted(pairs):
                likelihood = prior * likelihoods[0][:, index[a]] * likelihoods[1][:, index[b]]
                total = float(likelihood.sum())
                scores = likelihood/total if total > 0 else np.zeros(k, dtype='float64')
                choices[(a, b)] = (classes[int(np.argmax(likelihood))] if total > 0 else 0, scores)
                # Stable tie order: smallest numerical class code first. Zero score is not a candidate.
                order = np.argsort(-scores, kind='stable')
                ranks[(a, b)] = [(int(classes[j]), float(scores[j])) for j in order if scores[j] > 0][:3]
            # Indexed lookup is exact for identical input class pairs. Output is a much
            # smaller raster than five additional rasters over a sparse country extent.
            pair_ids = {pair: idx for idx, pair in enumerate(sorted(pairs), start=1)}
            if coverage_policy == 'either':
                # Encode fallback combinations too; their confidence is undefined.
                for code in sorted(set(ml_counts) | set(rb_counts)):
                    for pair in ((code, 0), (0, code)):
                        if pair not in pair_ids:
                            pair_ids[pair] = len(pair_ids) + 1
            profile = ml.profile.copy()
            profile.update(driver='GTiff', count=1, dtype='int32', nodata=0, compress='deflate',
                        tiled=True, blockxsize=256, blockysize=256, BIGTIFF='IF_SAFER')
            method_profile = profile.copy()
            method_profile.update(dtype='uint8', nodata=0)
            result_counts = Counter()
            top3_summary = {'top3_layout': top3_layout,
                            'ranking': 'descending normalized V2 model score; ties ascending numeric class',
                            'probability_interpretation': 'relative fusion model score, not calibrated accuracy',
                            'zero_score_classes_excluded': True,
                            'single_model_fallback_scores': 'undefined (NoData)'}
            method_counts = Counter()
            with ExitStack() as stack:
                classification = stack.enter_context(rasterio.open(targets[0], 'w', **profile))
                method_ds = stack.enter_context(rasterio.open(targets[1], 'w', **method_profile))
                method_ds.update_tags(method_0='no_data', method_3='uniform_V2_fusion', method_4='ml_only', method_5='rb_only')
                top3_writers = {}
                if top3_layout in ('rasters', 'both'):
                    prob_profile = profile.copy()
                    prob_profile.update(dtype='float32', nodata=-9999.0)
                    for name in top3_names:
                        selected_profile = prob_profile if 'probability' in name else profile
                        top3_writers[name] = stack.enter_context(rasterio.open(output_dir/name, 'w', **selected_profile))
                index_ds = None
                if top3_layout in ('index', 'both'):
                    index_profile = profile.copy()
                    index_profile.update(dtype='uint32', nodata=0)
                    index_ds = stack.enter_context(rasterio.open(output_dir/'prediction_pair_index.tif', 'w', **index_profile))
                    index_ds.update_tags(interpretation='lookup key for prediction_pair_lookup.csv',
                                        index_0='outside fusion domain or missing both predictions')
                confidence_ds = None
                if write_confidence:
                    conf_profile = profile.copy()
                    conf_profile.update(dtype='float32', nodata=None, count=k)
                    confidence_ds = stack.enter_context(rasterio.open(output_dir/'hybridMap_confidence.tif', 'w', **conf_profile))
                    for n, code in enumerate(classes, 1):
                        confidence_ds.set_band_description(n, f'class_{code}')
                    confidence_ds.update_tags(class_codes=','.join(map(str, classes)), interpretation='normalized_fusion_model_score')
                for window in FusionHM._blocks(ml.width, ml.height, block_size):
                    a, av = FusionHM._read(ml, window)
                    b, bv = FusionHM._read(rb, window)
                    both = av & bv
                    output = np.zeros(a.shape, dtype='int32')
                    methods = np.zeros(a.shape, dtype='uint8')
                    confidence = np.zeros((k, *a.shape), dtype='float32') if write_confidence else None
                    ranked_blocks = None
                    if top3_writers:
                        ranked_blocks = {name: np.full(a.shape, -9999.0 if 'probability' in name else 0,
                                                    dtype='float32' if 'probability' in name else 'int32') for name in top3_names}
                    index_block = np.zeros(a.shape, dtype='uint32') if index_ds is not None else None
                    if both.any():
                        for pair in np.unique(np.column_stack((a[both], b[both])), axis=0):
                            x, y = map(int, pair)
                            selected, scores = choices[(x, y)]
                            match = both & (a == x) & (b == y)
                            output[match] = selected
                            if index_block is not None:
                                index_block[match] = pair_ids[(x, y)]
                            if ranked_blocks is not None:
                                for rank_idx, (class_code, prob) in enumerate(ranks[(x, y)]):
                                    if rank_idx == 0:
                                        ranked_blocks['top1_probability.tif'][match] = prob
                                    else:
                                        ranked_blocks[f'top{rank_idx+1}_classification.tif'][match] = class_code
                                        ranked_blocks[f'top{rank_idx+1}_probability.tif'][match] = prob
                            if selected:
                                methods[match] = 3
                                if confidence is not None:
                                    confidence[:, match] = scores.astype('float32')[:, None]
                    if coverage_policy == 'either':
                        only_ml, only_rb = av & ~bv, bv & ~av
                        output[only_ml] = a[only_ml]; methods[only_ml] = 4
                        output[only_rb] = b[only_rb]; methods[only_rb] = 5
                        if index_block is not None:
                            for code in np.unique(a[only_ml]):
                                index_block[only_ml & (a == code)] = pair_ids[(int(code), 0)]
                            for code in np.unique(b[only_rb]):
                                index_block[only_rb & (b == code)] = pair_ids[(0, int(code))]
                    classification.write(output, 1, window=window)
                    if index_ds is not None:
                        index_ds.write(index_block, 1, window=window)
                    if ranked_blocks is not None:
                        for name, dataset in top3_writers.items():
                            dataset.write(ranked_blocks[name], 1, window=window)
                    method_ds.write(methods, 1, window=window)
                    if confidence_ds is not None:
                        confidence_ds.write(confidence, window=window)
                    for code, count in zip(*np.unique(output[output != 0], return_counts=True)):
                        result_counts[int(code)] += int(count)
                    for method, count in zip(*np.unique(methods, return_counts=True)):
                        method_counts[int(method)] += int(count)
        if top3_layout in ('index', 'both'):
            with (output_dir/'prediction_pair_lookup.csv').open('w', newline='', encoding='utf-8') as stream:
                writer = csv.writer(stream)
                writer.writerow(['index', 'ml_class', 'rb_class', 'method', 'top1_class', 'top1_probability',
                                'top2_class', 'top2_probability', 'top3_class', 'top3_probability'])
                for pair, pair_id in sorted(pair_ids.items(), key=lambda entry: entry[1]):
                    if pair in ranks:
                        ranked = ranks[pair]
                        fields = []
                        for i in range(3):
                            fields.extend(ranked[i] if i < len(ranked) else ('', ''))
                        writer.writerow([pair_id, pair[0], pair[1], 'v2_fusion', *fields])
                    else:
                        code = pair[0] or pair[1]
                        writer.writerow([pair_id, *pair, 'single_model_fallback', code, '', '', '', '', ''])
        summary = {'variant':'V2_uniform', 'coverage_policy':coverage_policy, 'number_of_classes':k,
                'classes':classes, 'uniform_value':1/k, 'empty_ml_reference_rows':empty_rows[0],
                'empty_rb_reference_rows':empty_rows[1], 'reference_observations_ml':float(ml_cm.to_numpy().sum()),
                'reference_observations_rb':float(rb_cm.to_numpy().sum()),
                'shared_valid_pixels':sum(pairs.values()), 'observed_prediction_pairs':len(pairs),
                'output_class_counts':dict(result_counts), 'method_counts':dict(method_counts),
                'prior_definition':('(ML class count + RB class count)/(2 * common valid pixels)' if coverage_policy == 'both' else '(ML class count + RB class count)/(2 * full raster pixels)'),
                'confidence':'model scores; not independently calibrated',
                'top3':top3_summary, 'prior_scope': 'context_global' if global_stats is not None else 'single_input_extent'}
        targets[2].write_text(json.dumps(summary, indent=2), encoding='utf-8')
        return summary

    def transform(self, context, flowfile):
        self.logger.info("================= Starting FusionHM Processor =================")
        if flowfile is None:
            self.logger.error("Incoming flowfile is null")
            return FlowFileTransformResult(relationship="failure")

        ml_confusion_matrix_path = context.getProperty(self.ml_confusion_matrix_path).getValue()
        if ml_confusion_matrix_path is None or not ml_confusion_matrix_path.strip():
            ml_confusion_matrix_path = flowfile.getAttribute("ml_confusion_matrix_path")
        if ml_confusion_matrix_path is None or not ml_confusion_matrix_path.strip():
            self.logger.error("ML Confusion Matrix Path property missing from the processor property and FlowFile attribute 'ml.confusion.matrix.path'")
            return FlowFileTransformResult(relationship="failure")

        rb_confusion_matrix_path = context.getProperty(self.rb_confusion_matrix_path).getValue()
        if rb_confusion_matrix_path is None or not rb_confusion_matrix_path.strip():
            rb_confusion_matrix_path = flowfile.getAttribute("rb_confusion_matrix_path")
        if rb_confusion_matrix_path is None or not rb_confusion_matrix_path.strip():
            self.logger.error("RB Confusion Matrix Path property missing from the processor property and FlowFile attribute 'rb.confusion.matrix.path'")
            return FlowFileTransformResult(relationship="failure")

        context_json = context.getProperty(self.context_json).getValue()
        if context_json is None or not context_json.strip():
            context_json = flowfile.getAttribute("fusion_context_geojson")
        if context_json is None or not context_json.strip():
            self.logger.error("Context GeoJSON property missing from the processor property and FlowFile attribute 'fusion_context_geojson'")
            return FlowFileTransformResult(relationship="failure")

        ml_raster_paths = context.getProperty(self.ml_raster_paths).getValue()
        if ml_raster_paths is None or not ml_raster_paths.strip():
            ml_raster_paths = flowfile.getAttribute("ml_raster_path")
        if ml_raster_paths is None or not ml_raster_paths.strip():
            self.logger.error("ML Raster Path property missing from the processor property and FlowFile attribute 'ml_raster_path'")
            return FlowFileTransformResult(relationship="failure")

        rb_raster_paths = context.getProperty(self.rb_raster_paths).getValue()
        if rb_raster_paths is None or not rb_raster_paths.strip():
            rb_raster_paths = flowfile.getAttribute("rb_raster_path")
        if rb_raster_paths is None or not rb_raster_paths.strip():
            self.logger.error("RB Raster Path property missing from the processor property and FlowFile attribute 'rb_raster_path'")
            return FlowFileTransformResult(relationship="failure")

        try:
            root = Path("/opt/nifi/output")
            if not context_json:
                raise ValueError("Missing bounding geometry in FlowFile attribute 'fusion_context_geojson'")
            context_geojson = json.loads(context_json)

            from spatial_inputs import MosaicSource, read_context
            geometry = read_context(context_geojson, raster_paths=ml_raster_paths + rb_raster_paths)
            with ExitStack() as stack:
                ml_mosaic = stack.enter_context(
                    MosaicSource(ml_raster_paths, geometry, overlap_policy='error')
                )
                rb_mosaic = stack.enter_context(
                    MosaicSource(rb_raster_paths, geometry, reference=ml_mosaic, overlap_policy='error')
                )
                summary = FusionHM.fuse_v2(
                    ml_path=ml_mosaic,
                    rb_path=rb_mosaic,
                    ml_cm=pd.read_csv(ml_confusion_matrix_path, index_col=0),
                    rb_cm=pd.read_csv(rb_confusion_matrix_path, index_col=0),
                    output_dir=root,
                    block_size=512,
                    coverage_policy='both',
                    write_confidence=True,
                    overwrite=True,
                    top3_layout='both',
                    global_stats=None
                )

            self.logger.info(f"FusionHM Processor completed successfully. Summary: {summary}")

            return FlowFileTransformResult(
                relationship="success",
                attributes={
                    "hybridmap.classification.path": str(root / "HM/hybridMap_classification.tif"),
                    "classification.method.path": str(root / "HM/classification_method.tif")
                })  

        except Exception as e:
            import traceback
            self.logger.error(f"An error occurred while creating confusion matrices: {e}")
            traceback.print_exc()
            return FlowFileTransformResult(
                relationship="failure",
                attributes={"error": str(e)}
            )