"""Context-clipped, lazily windowed mosaic inputs and point-local validation matrices.

No STAC access, no modification of input files. GeoJSON context geometries are
reprojected to EPSG:4326 in memory when a source CRS is declared.
"""
from __future__ import annotations

from collections import Counter
from contextlib import ExitStack
import json
import logging
import os
import re
from pathlib import Path

import numpy as np
import pandas as pd
import rasterio
from rasterio.features import geometry_mask
from rasterio.transform import Affine, rowcol
from rasterio.vrt import WarpedVRT
from rasterio.warp import Resampling, transform_bounds
from rasterio.windows import Window, bounds as window_bounds, transform as window_transform
from shapely.geometry import box, mapping as geom_mapping, shape
from shapely.ops import transform as geom_transform, unary_union
from pyproj import CRS, Transformer


def _context_source_crs(doc):
    """Read optional legacy GeoJSON CRS; RFC 7946 GeoJSON defaults to WGS84.

    Accepts e.g. {"type":"name","properties":{"name":"EPSG:3857"}}
    or {"type":"EPSG","properties":{"code":3857}}.
    """
    declared = doc.get('crs')
    if declared is None:
        return CRS.from_epsg(4326), False
    if isinstance(declared, str):
        crs_spec = declared
    elif isinstance(declared, dict):
        props = declared.get('properties') or {}
        if not isinstance(props, dict):
            raise ValueError('Invalid GeoJSON CRS properties; expected an object')
        if str(declared.get('type', '')).upper() == 'EPSG':
            code = props.get('code')
            crs_spec = f'EPSG:{code}' if code is not None else None
        else:
            crs_spec = props.get('name')
            if crs_spec is None and props.get('code') is not None:
                crs_spec = f"EPSG:{props['code']}"
    else:
        crs_spec = None
    if not crs_spec:
        raise ValueError('Context GeoJSON declares a CRS but it is not understood; use an EPSG name such as EPSG:3857')
    try:
        return CRS.from_user_input(crs_spec), True
    except Exception as exc:
        raise ValueError(f'Unrecognized GeoJSON context CRS {crs_spec!r}; cannot safely reproject') from exc


def _infer_context_crs_from_tiles(geometry, raster_paths):
    """Choose a source CRS only when tile metadata supports one unambiguous candidate.

    Source geometry is projected *as-is* in each candidate raster CRS, and
    compared with actual tile bounds. Never guess an EPSG code from magnitudes.
    """
    candidate_tiles = {}
    for path in raster_paths or ():
        with rasterio.open(path) as src:
            if src.crs is None:
                continue
            crs = CRS.from_user_input(src.crs)
            key = crs.to_string()
            candidate_tiles.setdefault(key, []).append(box(*src.bounds))
    scores = []
    for key, footprints in candidate_tiles.items():
        crs = CRS.from_user_input(key)
        if crs.is_geographic:
            continue
        coverage = unary_union(footprints)
        intersection = geometry.intersection(coverage)
        if geometry.area <= 0:
            continue
        ratio = intersection.area / geometry.area
        if ratio > 0.001:
            scores.append((ratio, key))
    scores.sort(reverse=True)
    if not scores:
        raise ValueError('GeoJSON CRS metadata is inconsistent with its coordinates, and no projected ML/RB tile CRS overlaps the original polygon. Provide HM_CONTEXT_SOURCE_CRS after identifying the true source CRS.')
    if len(scores) > 1 and scores[1][0] >= 0.9 * scores[0][0]:
        raise ValueError(f'Ambiguous context CRS: candidate tile CRS matches {scores[:3]}. Supply HM_CONTEXT_SOURCE_CRS explicitly.')
    logging.getLogger(__name__).warning('Context GeoJSON CRS metadata is inconsistent; inferred %s from tile metadata (coverage of polygon %.1f%%). Confirm this matches the GIS source CRS.', scores[0][1], scores[0][0]*100)
    return CRS.from_user_input(scores[0][1])


def read_context(path, source_crs_override=None, raster_paths=None):
    """Return a polygon in EPSG:4326, transforming in memory.

    Set HM_CONTEXT_SOURCE_CRS=EPSG:XXXXX when GeoJSON CRS metadata is wrong.
    An explicit argument takes precedence over the environment variable.
    Never infer a source CRS from coordinate magnitude alone.
    """
    #we first differentiate between having a path or a geojson
    if isinstance(path, dict):
        doc = path
    elif isinstance(path, str) and path.lstrip().startswith('{'):
        doc = json.loads(path)
    else:
        doc = json.loads(Path(path).read_text(encoding='utf-8'))
    declared_crs, has_declared_crs = _context_source_crs(doc)
    override = source_crs_override or os.environ.get('HM_CONTEXT_SOURCE_CRS')
    if override:
        try:
            source_crs = CRS.from_user_input(override)
        except Exception as exc:
            raise ValueError(f'Invalid HM_CONTEXT_SOURCE_CRS {override!r}; supply the actual EPSG code of the source coordinates') from exc
        logging.getLogger(__name__).warning(
            'Using explicit context source CRS %s instead of declared %s; source GeoJSON is unchanged',
            source_crs.to_string(), declared_crs.to_string()
        )
        has_declared_crs = True
    else:
        source_crs = declared_crs
    if doc.get('type') == 'FeatureCollection':
        features = [f['geometry'] for f in doc.get('features', []) if f.get('geometry')]
    elif doc.get('type') == 'Feature':
        features = [doc['geometry']]
    elif doc.get('type') == 'GeometryCollection':
        features = doc.get('geometries', [])
    else:
        features = [doc]
    geoms = [shape(g) for g in features]
    if not geoms:
        raise ValueError('Context GeoJSON has no geometries')
    if any(not x.is_valid for x in geoms):
        raise ValueError('Context GeoJSON has invalid geometries; repair before running')
    if any(x.geom_type not in ('Polygon', 'MultiPolygon') for x in geoms):
        raise ValueError('Context must be a polygon or multipolygon')
    merged = unary_union(geoms)
    if merged.is_empty:
        raise ValueError('Context is empty')

    if (not override and source_crs.is_geographic and
            (merged.bounds[0] < -180 or merged.bounds[2] > 180 or
             merged.bounds[1] < -90 or merged.bounds[3] > 90)):
        source_crs = _infer_context_crs_from_tiles(merged, raster_paths)

    if source_crs != CRS.from_epsg(4326):
        transformer = Transformer.from_crs(source_crs, 'EPSG:4326', always_xy=True)
        merged = geom_transform(transformer.transform, merged)
        logging.getLogger(__name__).info(
            'Reprojected GeoJSON context in memory: %s -> EPSG:4326 (input file unchanged)',
            source_crs.to_string(),
        )
    elif not has_declared_crs:
        logging.getLogger(__name__).info('GeoJSON context has no declared CRS; interpreting as EPSG:4326 per RFC 7946')

    if merged.is_empty or not merged.is_valid:
        raise ValueError('Context geometry is empty or invalid after CRS transformation')
    xmin, ymin, xmax, ymax = merged.bounds
    if not (all(np.isfinite(v) for v in (xmin, ymin, xmax, ymax)) and
            -180 <= xmin <= xmax <= 180 and -90 <= ymin <= ymax <= 90):
        detail = ('No CRS is declared; coordinate units look non-geographic. '
                  'Export the GeoJSON with a named CRS (or in EPSG:4326). '
                  if not has_declared_crs else 'Check that the declared context CRS is correct. ')
        raise ValueError(f'Context coordinates are outside EPSG:4326 longitude/latitude bounds. {detail}')
    return merged


def discover_tiles(folder, explicit, directory, pattern):
    if explicit:
        paths = [Path(p).expanduser().resolve() for p in explicit]
    elif directory:
        base = Path(directory).expanduser().resolve()
        if not base.is_dir():
            raise FileNotFoundError(f'Tile directory not found: {base}')
        paths = sorted({p.resolve() for p in base.rglob(pattern) if p.is_file()})
    else:
        if not folder.is_file():
            raise FileNotFoundError(f'Missing single-file raster {folder}; supply --ml-tiles-dir or --rb-tiles-dir')
        paths = [folder.resolve()]
    if not paths:
        raise ValueError('No tiles found; check --*-tiles-dir and --*-glob')
    for path in paths:
        if not path.is_file():
            raise FileNotFoundError(f'Missing tile: {path}')
        if path.suffix.lower() not in ('.tif', '.tiff'):
            raise ValueError(f'Expected TIFF tile, got: {path}')
    return paths


def _probability_cube_bands(src, level=3):
    """Decode GET class codes from probability-cube band descriptions.

    Example: Level3_class-T1_habitat-T1.2-10102.  Raster probabilities
    are stored as numeric scores; only the relative ranking is needed.
    Returns (class_codes_in_ascending_order, one_based_band_indexes).
    """
    found = {}
    for index, description in enumerate(src.descriptions, 1):
        text = description or ''
        match = re.fullmatch(r'Level([123])_class-.*_habitat-.*-([0-9]+)', text)
        if not match or int(match.group(1)) != level:
            continue
        code = int(match.group(2))
        if code in found:
            raise ValueError(f'Duplicate Level-{level} GET class {code} in probability cube {src.name}')
        found[code] = index
    if not found:
        raise ValueError(f'No Level-{level} GET class bands found in probability cube {src.name}; check band descriptions')
    codes = tuple(sorted(found))
    return codes, tuple(found[code] for code in codes)


def _aligned(src, crs, transform, tolerance=1e-5):
    if src.crs != crs:
        return False
    t = src.transform
    if abs(t.b) > 1e-12 or abs(t.d) > 1e-12 or t.a <= 0 or t.e >= 0:
        return False
    if not (np.isclose(t.a, transform.a, rtol=1e-6) and np.isclose(t.e, transform.e, rtol=1e-6)):
        return False
    dx = (t.c-transform.c)/transform.a
    dy = (t.f-transform.f)/abs(transform.e)
    return abs(dx-round(dx)) < tolerance and abs(dy-round(dy)) < tolerance


class MosaicSource:
    """Expose Rasterio-like read(window) over tiles, exact GeoJSON centre-pixel mask."""
    def __init__(self, paths, context_geom_wgs84, reference=None, overlap_policy='error', allow_resampling=False, level=3):
        self.stack = ExitStack()
        self.paths = list(paths)
        self.tiles = []
        self.overlap_policy = overlap_policy
        try:
            native = [self.stack.enter_context(rasterio.open(path)) for path in paths]
            source_modes = []
            band_selections = []
            self.class_codes = None
            for path, src in zip(paths, native):
                if src.crs is None or src.nodata is None:
                    raise ValueError(f'Tile requires a CRS and declared NoData: {path}')
                if src.transform.a <= 0 or src.transform.e >= 0:
                    raise ValueError(f'Unsupported source tile orientation: {path}')
                dt = np.dtype(src.dtypes[0])
                if not (np.issubdtype(dt, np.integer) or np.issubdtype(dt, np.floating)):
                    raise ValueError(f'Unsupported numeric TIFF dtype {dt}: {path}')
                if src.count == 1:
                    source_modes.append('classification')
                    band_selections.append(None)
                elif reference is None:
                    codes, indexes = _probability_cube_bands(src, level=level)
                    if self.class_codes is None:
                        self.class_codes = codes
                    elif codes != self.class_codes:
                        raise ValueError(f'ML probability cubes have inconsistent Level-3 class sets: {path}')
                    source_modes.append('probability_cube')
                    band_selections.append(indexes)
                else:
                    raise ValueError(f'RB raster must be a single-band class map, found {src.count} bands: {path}')
            if len(set(source_modes)) != 1:
                raise ValueError('Do not mix single-band ML classifications with multi-band ML probability cubes in one tile directory')
            self.source_mode = source_modes[0]
            if self.source_mode == 'probability_cube':
                logging.getLogger(__name__).info(
                    'ML probability cubes: deriving Level-%d single-band classifications in memory '
                    'with argmax over %d class bands; zero-score or NoData pixels remain unclassified',
                    level, len(self.class_codes))
                self.class_codes_array = np.asarray(self.class_codes, dtype='int32')
            seed = reference if reference is not None else native[0]
            self.crs = seed.crs
            self.nodata = 0
            self.count = 1
            self.transform_seed = seed.transform
            if context_geom_wgs84 is None:
                if len(native) != 1:
                    raise ValueError('A GeoJSON context is required when using multiple raster tiles')
                self.width, self.height, self.transform = seed.width, seed.height, seed.transform
                self.geometry = None
            else:
                transformer = Transformer.from_crs('EPSG:4326', self.crs, always_xy=True)
                self.geometry = geom_transform(transformer.transform, context_geom_wgs84)
                xmin, ymin, xmax, ymax = self.geometry.bounds
                # Snap bounding box to the full reference pixel grid. No grid shifts across approaches.
                r0, c0 = rowcol(self.transform_seed, xmin, ymax, op=np.floor)
                r1, c1 = rowcol(self.transform_seed, xmax, ymin, op=np.ceil)
                r0, c0, r1, c1 = map(int, (r0,c0,r1,c1))
                self.transform = self.transform_seed * Affine.translation(c0, r0)
                self.width = c1-c0
                self.height = r1-r0
                if self.width <= 0 or self.height <= 0:
                    raise ValueError('GeoJSON context has empty projected pixel extent')
            self.profile = seed.profile.copy()
            self.profile.update(width=self.width, height=self.height, transform=self.transform, crs=self.crs, nodata=0, count=1)
            for path, src, indexes in zip(paths, native, band_selections):
                if not _aligned(src, self.crs, self.transform) and not allow_resampling:
                    raise ValueError(f'Tile {path} is not pixel-aligned with mosaic grid; use --allow-resampling to explicitly permit nearest-neighbour reprojection')
                # A probability of zero is a *valid score*.  Do not turn it
                # into NoData in the VRT; use the source cube's 255 sentinel.
                vrt_nodata = src.nodata if self.source_mode == 'probability_cube' else 0
                vrt = self.stack.enter_context(WarpedVRT(src, crs=self.crs, transform=self.transform,
                        width=self.width, height=self.height, resampling=Resampling.nearest,
                        nodata=vrt_nodata))
                bbox = transform_bounds(src.crs, self.crs, *src.bounds, densify_pts=21)
                self.tiles.append((path, vrt, box(*bbox), indexes))
        except Exception:
            self.stack.close()
            raise

    @property
    def shape(self):
        return (self.height, self.width)

    def inside_mask(self, window):
        if self.geometry is None:
            return np.ones((int(window.height), int(window.width)), dtype=bool)
        return geometry_mask([geom_mapping(self.geometry)], out_shape=(int(window.height), int(window.width)),
                             transform=window_transform(window, self.transform), invert=True, all_touched=False)

    def read(self, index=1, window=None, masked=False):
        if index != 1:
            raise ValueError('Mosaic is single-band')
        if window is None:
            window = Window(0,0,self.width,self.height)
        dims = (int(window.height), int(window.width))
        output = np.zeros(dims, dtype='int32')
        valid = np.zeros(dims, dtype=bool)
        extent = box(*window_bounds(window, self.transform))
        for path, vrt, tile_bounds, band_indexes in self.tiles:
            if not extent.intersects(tile_bounds):
                continue
            if self.source_mode == 'probability_cube':
                cube = vrt.read(indexes=band_indexes, window=window, masked=True)
                raw = np.asarray(cube.data)
                available = (~np.ma.getmaskarray(cube)) & np.isfinite(raw) & (raw >= 0)
                # Select only GET Level-3 bands.  Use class codes extracted
                # from per-band descriptions rather than physical band order.
                scores = np.where(available, raw.astype(np.float32, copy=False), -np.inf)
                winner = np.argmax(scores, axis=0)
                best = np.take_along_axis(scores, winner[None, ...], axis=0)[0]
                mask = np.isfinite(best) & (best > 0)
                data = self.class_codes_array[winner]
            else:
                tile = vrt.read(1, window=window, masked=True)
                mask = ~np.ma.getmaskarray(tile)
                data = np.asarray(tile.data)
                mask &= np.isfinite(data)
                mask &= data != 0
                valid_values = data[mask]
                if (np.any(valid_values <= 0) or np.any(valid_values > np.iinfo(np.int32).max) or
                    (np.issubdtype(valid_values.dtype, np.floating) and np.any(valid_values != np.floor(valid_values)))):
                    raise ValueError(f'Invalid class codes in tile {path}: require positive integer-valued Int32 codes')
            if not mask.any():
                continue
            mismatches = valid & mask & (output != data)
            if mismatches.any() and self.overlap_policy == 'error':
                raise ValueError(f'Conflicting valid tile values ({mismatches.sum()} pixels) at {path}; use --overlap-policy first|last only if intentional')
            writable = mask & ~valid if self.overlap_policy == 'first' else mask
            output[writable] = data[writable].astype('int32')
            valid |= mask
        valid &= self.inside_mask(window)
        return np.ma.array(output, mask=~valid) if masked else np.where(valid, output, 0)

    def close(self):
        self.stack.close()

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()


def _reference_code(value, mapping):
    if pd.isna(value):
        return None
    token = str(value).strip()
    if token in mapping:
        return int(mapping[token])
    try:
        number = float(token)
        if np.isfinite(number) and number > 0 and number.is_integer():
            return int(number)
    except (TypeError, ValueError):
        pass
    return None


def read_validation_parquet(path, geometry, reference_col=None, x_col=None, y_col=None, xy_crs='EPSG:4326', year_col=None, year=None):
    import geopandas as gpd
    try:
        data = gpd.read_parquet(path)
        if data.crs is None:
            raise ValueError('GeoParquet geometry lacks a CRS; specify valid CRS in source metadata')
    except (ValueError, KeyError, TypeError) as exc:
        if not (x_col and y_col):
            raise ValueError(f'Cannot read spatial GeoParquet ({exc}). For ordinary Parquet specify --validation-x-column, --validation-y-column and --validation-crs.') from exc
        df = pd.read_parquet(path)
        if x_col not in df or y_col not in df:
            raise ValueError(f'Validation XY columns not found: {x_col}, {y_col}. Columns: {list(df.columns)}')
        data = gpd.GeoDataFrame(df, geometry=gpd.points_from_xy(df[x_col], df[y_col]), crs=xy_crs)
    invalid = data.geometry.isna() | data.geometry.is_empty
    data = data.loc[~invalid].copy()
    source_rows = len(data)
    if year is not None:
        if year_col not in data.columns:
            raise ValueError(f'Validation year column {year_col!r} not found. Available: {list(data.columns)}')
        column = data[year_col]
        if pd.api.types.is_datetime64_any_dtype(column):
            years = column.dt.year
        else:
            numeric = pd.to_numeric(column, errors='coerce')
            years = numeric.where((numeric >= 1800) & (numeric <= 2200) & (numeric % 1 == 0))
            date_rows = years.isna() & column.notna()
            if date_rows.any():
                parsed = pd.to_datetime(column.loc[date_rows], errors='coerce', utc=True)
                years.loc[date_rows] = parsed.dt.year
        data = data.loc[years == year].copy()
    after_year = len(data)
    if any(kind != 'Point' for kind in data.geometry.geom_type.unique()):
        raise ValueError('Validation Parquet must contain POINT geometries, one per reference observation')
    in_wgs84 = data.to_crs('EPSG:4326')
    selected = in_wgs84.loc[in_wgs84.geometry.intersects(geometry)].copy()
    return selected, {'source_rows': source_rows, 'after_year_filter_rows': after_year,
                      'excluded_other_year': source_rows - after_year, 'validation_year_filter': year,
                      'validation_year_column': year_col, 'inside_context_rows': len(selected),
                      'excluded_outside': after_year-len(selected), 'source_columns': list(data.columns),
                      'reference_column': reference_col, 'geometry_source_crs': str(data.crs)}


def sample_validation(selected, ml, rb, mapping, reference_col, sample_tile_size=512, require_both=True):
    if reference_col not in selected.columns:
        raise ValueError(f'Reference class column {reference_col!r} missing. Columns: {list(selected.columns)}')
    projected = selected.to_crs(ml.crs)
    coords = [(pt.x, pt.y) for pt in projected.geometry]
    rows, cols = rowcol(ml.transform, [p[0] for p in coords], [p[1] for p in coords])
    rows = np.asarray(rows); cols = np.asarray(cols)
    codes = [_reference_code(value, mapping) for value in selected[reference_col]]
    unmapped = Counter(str(value) for value, code in zip(selected[reference_col], codes) if code is None)
    if unmapped:
        raise ValueError(f'Validation class values missing from mapping CSV: {unmapped.most_common(12)}. Select the correct --validation-ref-column or update eo_maps.csv.')
    groups = {}
    outside_grid = 0
    for i, (r,c) in enumerate(zip(rows,cols)):
        if 0 <= r < ml.height and 0 <= c < ml.width:
            groups.setdefault((int(r)//sample_tile_size,int(c)//sample_tile_size), []).append(i)
        else:
            outside_grid += 1
    pair_ml, pair_rb, ref_count, shared_ref_count = Counter(), Counter(), Counter(), Counter()
    sampled_ml = sampled_rb = sampled_both = sampled_ml_only = sampled_rb_only = sampled_neither = 0
    for (block_r, block_c), indices in groups.items():
        w = Window(block_c*sample_tile_size, block_r*sample_tile_size,
                   min(sample_tile_size, ml.width-block_c*sample_tile_size),
                   min(sample_tile_size, ml.height-block_r*sample_tile_size))
        a = ml.read(1, window=w, masked=True)
        b = rb.read(1, window=w, masked=True)
        for i in indices:
            rr = int(rows[i])-int(w.row_off)
            cc = int(cols[i])-int(w.col_off)
            ref = int(codes[i])
            ref_count[ref] += 1
            valid_ml = not np.ma.getmaskarray(a)[rr,cc]
            valid_rb = not np.ma.getmaskarray(b)[rr,cc]
            sampled_ml += int(valid_ml)
            sampled_rb += int(valid_rb)
            if valid_ml and valid_rb:
                sampled_both += 1
                shared_ref_count[ref] += 1
            elif valid_ml:
                sampled_ml_only += 1
            elif valid_rb:
                sampled_rb_only += 1
            else:
                sampled_neither += 1
            # In 'both' mode, ML and RB MUST use the exact same validation locations.
            eligible = (valid_ml and valid_rb) if require_both else True
            if eligible and valid_ml:
                pair_ml[ref, int(a.data[rr,cc])] += 1
            if eligible and valid_rb:
                pair_rb[ref, int(b.data[rr,cc])] += 1
    available = {'validation_points_inside': len(selected), 'validation_points_outside_mosaic_grid': outside_grid,
                 'reference_points_inside_grid': int(sum(ref_count.values())),
                 'ml_samples_with_valid_predictions': sampled_ml, 'rb_samples_with_valid_predictions': sampled_rb,
                 'validation_points_with_both_models': sampled_both,
                 'validation_points_with_ml_only': sampled_ml_only,
                 'validation_points_with_rb_only': sampled_rb_only,
                 'validation_points_with_neither_model': sampled_neither,
                 'ml_matrix_observations': int(sum(pair_ml.values())),
                 'rb_matrix_observations': int(sum(pair_rb.values())),
                 'sample_policy': 'both_models_valid' if require_both else 'each_model_independently',
                 'shared_reference_counts': {str(k): v for k, v in sorted(shared_ref_count.items())},
                 'reference_counts': {str(k): v for k, v in sorted(ref_count.items())}}
    return pair_ml, pair_rb, available


def build_cms(pairs_ml, pairs_rb, mapping, other_codes=()):
    codes = sorted({int(v) for v in mapping.values()} | set(map(int, other_codes)) |
                   {int(x) for pair in list(pairs_ml)+list(pairs_rb) for x in pair})
    if not pairs_ml or not pairs_rb:
        raise ValueError('Filtered validation points produce no valid predictions for one or both models')
    ml = pd.DataFrame(0, index=codes, columns=codes, dtype='int64')
    rb = ml.copy()
    for (reference, predicted), count in pairs_ml.items():
        ml.loc[reference, predicted] = count
    for (reference, predicted), count in pairs_rb.items():
        rb.loc[reference, predicted] = count
    return ml, rb


def nonoverlapping_tile_regions(ml, rb, coverage_policy='both'):
    """Disjoint source-footprint windows, without traversing empty context bbox.

    Return windows in the common mosaic grid. The exact polygon and valid-data
    masks remain enforced by MosaicSource.read() for every window.
    """
    from rasterio.windows import from_bounds
    if coverage_policy not in ('both', 'either'):
        raise ValueError('coverage_policy must be both or either')
    if ml.transform != rb.transform or ml.shape != rb.shape or ml.crs != rb.crs:
        raise ValueError('ML/RB mosaics must share the same grid')
    if ml.geometry is None:
        return [Window(0, 0, ml.width, ml.height)]
    footprints_a = [record[2] for record in ml.tiles]
    footprints_b = [record[2] for record in rb.tiles]
    if coverage_policy == 'both':
        candidates = (a.intersection(b).intersection(ml.geometry) for a in footprints_a for b in footprints_b)
    else:
        candidates = (g.intersection(ml.geometry) for g in footprints_a + footprints_b)
    occupied = []
    for geom in candidates:
        if geom.is_empty or geom.area <= 0:
            continue
        bounds = from_bounds(*geom.bounds, transform=ml.transform)
        r0 = max(0, int(np.floor(bounds.row_off)))
        c0 = max(0, int(np.floor(bounds.col_off)))
        r1 = min(ml.height, int(np.ceil(bounds.row_off + bounds.height)))
        c1 = min(ml.width, int(np.ceil(bounds.col_off + bounds.width)))
        if r0 >= r1 or c0 >= c1:
            continue
        pending = [(r0, c0, r1, c1)]
        for used in occupied:
            next_pending = []
            a0,b0,a1,b1 = used
            for x0,y0,x1,y1 in pending:
                top,bottom = max(x0,a0), min(x1,a1)
                left,right = max(y0,b0), min(y1,b1)
                if top >= bottom or left >= right:
                    next_pending.append((x0,y0,x1,y1))
                else:
                    next_pending.extend([(x0,y0,top,y1),(bottom,y0,x1,y1),
                                         (top,y0,bottom,left),(top,right,bottom,y1)])
            pending = [r for r in next_pending if r[0] < r[2] and r[1] < r[3]]
            if not pending:
                break
        occupied.extend(pending)
    return [Window(c0,r0,c1-c0,r1-r0) for r0,c0,r1,c1 in occupied]