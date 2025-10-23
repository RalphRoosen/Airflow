# -*- coding: utf-8 -*-
"""
Herbruikbare util-functies voor WFS-verkeer en Postgres inserts.

- iter_wfs_batches():   pagineert WFS 1.1.0 / 2.0.0 requests (met optionele 50k-cap)
- fetch_page():         één pagina ophalen (POST bij CQL/URL-lang; met auto-geometry-fallback)
- flush_values():       execute_values wrapper
- _num(), _bool():      typehelpers
- build_cql_intersects(): maak CQL filter met polygoon (INTERSECTS)
- Geometry cache: per (base_url, typename) wordt de gevonden geometry-attribuutnaam onthouden

Aanpassingen:
- WFS 1.1.0: iteratieve paginatie met vendor-param startIndex + maxFeatures.
- Auto sortBy=fid bij 1.1.0 (indien niet opgegeven) om “natural order” error te voorkomen.
- wfs_hits: gebruikt POST bij lange CQL/URL, en auto-geometry-fallback bij “Illegal property name”.
- fetch_page: geen ‘None’ meer; altijd (json, version) of exception.
"""

import re
import time
import xml.etree.ElementTree as ET
from urllib.parse import urlencode

import requests
from psycopg2.extras import execute_values


# ─────────────────────────────────────────────────────────────────────────────
# Exceptions
# ─────────────────────────────────────────────────────────────────────────────

class WfsBboxCqlConflict(Exception):
    """Server meldt dat bbox en cql_filter tegelijk zijn opgegeven en elkaar uitsluiten."""
    pass

class WfsServerException(RuntimeError):
    """OWS/GeoServer exception (ook bij HTTP 200)."""
    pass


# ─────────────────────────────────────────────────────────────────────────────
# Type helpers
# ─────────────────────────────────────────────────────────────────────────────

def _num(v):
    if v in (None, "", "null"):
        return None
    try:
        return float(v)
    except Exception:
        return None

def _bool(v):
    if v is None:
        return None
    if isinstance(v, bool):
        return v
    s = str(v).strip().lower()
    if s in ("true", "t", "1", "yes", "y", "ja"):
        return True
    if s in ("false", "f", "0", "no", "n", "nee"):
        return False
    return None


# ─────────────────────────────────────────────────────────────────────────────
# Postgres helper
# ─────────────────────────────────────────────────────────────────────────────

def flush_values(pg_hook, sql: str, template: str, rows):
    if not rows:
        return
    conn = pg_hook.get_conn()
    with conn, conn.cursor() as cur:
        execute_values(cur, sql, rows, template=template)
    print("[DB] {} rijen geflusht.".format(len(rows)))


# ─────────────────────────────────────────────────────────────────────────────
# Logging helpers
# ─────────────────────────────────────────────────────────────────────────────

def _show_response_prefix(resp, maxlen=500):
    try:
        ct = resp.headers.get("content-type", "")
        snippet = resp.text[:maxlen]
        print("[DEBUG] content-type={}".format(ct))
        print("[DEBUG] body prefix:\n{}".format(snippet))
    except Exception as e:
        print("[DEBUG] cannot print response body: {!r}".format(e))

def _is_ows_exception(text: str) -> bool:
    if not text:
        return False
    t = text.lower()
    return ("exceptionreport" in t) or ("ows:exceptionreport" in t)

def _is_illegal_property(text: str) -> bool:
    return bool(text) and ("illegal property name" in text.lower())

def _is_natural_order_err(text: str) -> bool:
    return bool(text) and ("natural order" in text.lower())


# ─────────────────────────────────────────────────────────────────────────────
# HTTP helpers
# ─────────────────────────────────────────────────────────────────────────────

def _should_use_post(base_url: str, params: dict) -> bool:
    """
    Gebruik POST als:
    - er een CQL-filter aanwezig is (grote WKT), of
    - de resulterende URL > ~1800 tekens wordt.
    """
    if params and any(k in params for k in ("cql_filter", "CQL_FILTER")):
        return True
    try:
        qs = urlencode(params or {}, doseq=True)
        full = "{}?{}".format(base_url, qs)
        return len(full) > 1800
    except Exception:
        return True

def _ensure_geojson_output(output_format: str):
    allowed = ("application/json", "application/json; subtype=geojson", "json")
    return output_format if output_format in allowed else "application/json"

def _raise_if_bbox_cql_conflict(resp, typename):
    try:
        txt = resp.text or ""
    except Exception:
        txt = ""
    if txt:
        low = txt.lower()
        if "bbox" in low and "cql_filter" in low and "mutually exclusive" in low:
            raise WfsBboxCqlConflict(
                f"WFS server meldt conflict: bbox en cql_filter zijn tegelijk opgegeven (mutually exclusive) voor layer '{typename}'."
            )


# ─────────────────────────────────────────────────────────────────────────────
# Geometry cache / detection
# ─────────────────────────────────────────────────────────────────────────────

_GEOM_ATTR_CACHE = {}  # (base_url, typename) -> geometry attribuutnaam (str)

def _cache_key(base_url: str, typename: str) -> str:
    return f"{base_url}|{typename}"

def _get_cached_geom_attr(base_url: str, typename: str):
    return _GEOM_ATTR_CACHE.get(_cache_key(base_url, typename))

def _set_cached_geom_attr(base_url: str, typename: str, attr: str):
    if attr:
        _GEOM_ATTR_CACHE[_cache_key(base_url, typename)] = attr

def _detect_geom_attrs(base_url, typename, version="2.0.0", timeout=60):
    params = {
        "service": "WFS",
        "version": version,
        "request": "DescribeFeatureType",
        ("typeNames" if version == "2.0.0" else "typeName"): typename,
    }
    r = requests.get(base_url, params=params, timeout=timeout)
    r.raise_for_status()

    root = ET.fromstring(r.content)
    ns = {
        "xsd": "http://www.w3.org/2001/XMLSchema",
        "gml": "http://www.opengis.net/gml",
        "gml3": "http://www.opengis.net/gml/3.2",
    }

    candidates = []
    for el in root.findall(".//xsd:element", ns):
        t = el.get("type") or ""
        name = el.get("name") or ""
        if not name:
            continue
        if any(k in t for k in [
            "gml:Geometry", "gml:Point", "gml:Line", "gml:MultiLine",
            "gml:Polygon", "gml:MultiPolygon", "gml3:AbstractGeometry",
        ]):
            candidates.append(name)

    seen, out = set(), []
    for c in candidates:
        if c not in seen:
            seen.add(c)
            out.append(c)

    if not out:
        out = ["geom", "the_geom", "geometrie", "geometrie2d", "geografischeligging"]
    return out

_INTERSECTS_GEOM_RE = re.compile(r"^\s*INTERSECTS\(\s*([a-zA-Z_][\w.]*)\s*,", re.IGNORECASE)
def _replace_geom_attr_in_cql(cql_filter: str, new_attr: str) -> str:
    def repl(_m): return f"INTERSECTS({new_attr}, "
    return _INTERSECTS_GEOM_RE.sub(repl, cql_filter, count=1)


# ─────────────────────────────────────────────────────────────────────────────
# wfs_hits  (nu veilig met POST en auto-geometry-fallback)
# ─────────────────────────────────────────────────────────────────────────────

def wfs_hits(base_url, typename, version, srs, cql_filter=None, bbox=None, timeout=120):
    params = {
        "service": "WFS",
        "version": version,
        "request": "GetFeature",
        "srsName": srs,
        "resultType": "hits",
        ("typeNames" if version == "2.0.0" else "typeName"): typename,
    }
    if cql_filter:
        params["CQL_FILTER"] = cql_filter
    elif bbox:
        params["bbox"] = bbox

    def _do(params_):
        use_post = _should_use_post(base_url, params_)
        return requests.post(base_url, data=params_, timeout=timeout) if use_post \
             else requests.get(base_url, params=params_, timeout=timeout)

    try:
        r = _do(params)
        if r.status_code == 400 and _is_ows_exception(r.text) and _is_illegal_property(r.text) and cql_filter:
            # detecteer juiste geom en retry met POST
            try:
                real_geom = _detect_geom_attrs(base_url, typename, version=version, timeout=timeout)[0]
                params["CQL_FILTER"] = _replace_geom_attr_in_cql(cql_filter, real_geom)
                r = _do(params)
            except Exception as ee:
                print(f"[ERROR] DescribeFeatureType faalde in wfs_hits: {ee}")
                r.raise_for_status()
        r.raise_for_status()
    except Exception as e:
        print(f"[ERROR] WFS-hits request mislukt: {e}")
        return None

    txt = r.text or ""
    m = re.search(r'numberMatched="(\d+)"', txt) if version == "2.0.0" else re.search(r'numberOfFeatures="(\d+)"', txt)
    if m:
        total = int(m.group(1))
        print(f"[INFO] WFS-hits voor {typename}: {total} features (version={version})")
        return total

    print("[WARN] Kon geen 'hits'-totaal bepalen uit WFS response.")
    print(txt[:400] + ("..." if len(txt) > 400 else ""))
    return None


# ─────────────────────────────────────────────────────────────────────────────
# Core fetch + pagination
# ─────────────────────────────────────────────────────────────────────────────

def fetch_page(base_url, typename, bbox, start_index, count, version,
               srs="EPSG:28992", output_format="application/json",
               timeout=180, headers=None, extra_params=None, *,
               sort_by_11: str = "fid"):
    """
    Haal één pagina features op via WFS.
    - v2.0.0: startIndex/count
    - v1.1.0: maxFeatures + (vendor) startIndex; bij pagina's > 0 dwingen we sortBy (default 'fid').
    Retourneert (json, version) of raise't een exception.
    """
    output_format = _ensure_geojson_output(output_format)

    if version == "2.0.0":
        params = {
            "service": "WFS", "version": "2.0.0", "request": "GetFeature",
            "typeNames": typename, "srsName": srs, "outputFormat": output_format,
            "startIndex": start_index, "count": count,
        }
    else:
        params = {
            "service": "WFS", "version": "1.1.0", "request": "GetFeature",
            "typeName": typename, "srsName": srs, "outputFormat": output_format,
            "maxFeatures": count,
        }
        if start_index and start_index > 0:
            params["startIndex"] = start_index
        # Als geen sortBy meegegeven is, forceer 'fid' voor stabiele natural order
        if (start_index and start_index > 0) and (not extra_params or "sortBy" not in extra_params):
            params["sortBy"] = sort_by_11

    if bbox is not None:
        params["bbox"] = "{},{},{},{},{}".format(
            bbox["xmin"], bbox["ymin"], bbox["xmax"], bbox["ymax"], srs
        )
    if extra_params:
        params.update(extra_params)

    cached_geom = _get_cached_geom_attr(base_url, typename)
    if cached_geom and "cql_filter" in params:
        params["cql_filter"] = _replace_geom_attr_in_cql(params["cql_filter"], cached_geom)

    def _do(p):
        use_post = _should_use_post(base_url, p)
        return requests.post(base_url, data=p, timeout=timeout, headers=headers) if use_post \
             else requests.get(base_url, params=p, timeout=timeout, headers=headers)

    r = _do(params)

    # Niet-200 → behandel bekende fouten en raise
    if r.status_code != 200:
        _show_response_prefix(r)
        _raise_if_bbox_cql_conflict(r, typename)
        if (r.status_code == 400) and _is_ows_exception(r.text) and _is_illegal_property(r.text) and "cql_filter" in params:
            return _retry_with_detected_geom(base_url, typename, version, params, r, headers, timeout)
        r.raise_for_status()

    # 200 OK → kan alsnog OWS ExceptionReport zijn
    body = r.text or ""
    if _is_ows_exception(body):
        if _is_illegal_property(body) and "cql_filter" in params:
            print(f"[ERROR] WFS {version} OWS ExceptionReport: Illegal property → retry met DescribeFeatureType.")
            return _retry_with_detected_geom(base_url, typename, version, params, r, headers, timeout)
        if version == "1.1.0" and _is_natural_order_err(body):
            # Forceer sortBy en retry één keer
            if "sortBy" not in params:
                params["sortBy"] = sort_by_11
                print(f"[INFO] Retry 1.1.0 met expliciet sortBy={sort_by_11} i.v.m. 'natural order' fout.")
                r = _do(params)
                if _is_ows_exception(r.text or ""):
                    _show_response_prefix(r, maxlen=220)
                    raise WfsServerException("WFS 1.1.0 blijft 'natural order' fouten geven ondanks sortBy.")
                try:
                    return r.json(), version
                except ValueError:
                    _show_response_prefix(r)
                    raise WfsServerException("WFS 1.1.0 gaf geen JSON terug na sortBy-retry.")
        # Andere OWS errors
        _show_response_prefix(r, maxlen=220)
        raise WfsServerException("WFS gaf OWS ExceptionReport terug.")

    # Succespad: parse JSON
    try:
        return r.json(), version
    except ValueError:
        _show_response_prefix(r)
        # Als tóch illegal property: retry met detected geom
        if _is_illegal_property(body) and "cql_filter" in params:
            return _retry_with_detected_geom(base_url, typename, version, params, r, headers, timeout)
        raise WfsServerException("Response is not JSON. Check outputFormat / server.")


def _retry_with_detected_geom(base_url, typename, version, original_params, first_response, headers, timeout):
    try:
        candidates = _detect_geom_attrs(base_url, typename, version=version, timeout=timeout)
    except Exception as e:
        print("[WARN] DescribeFeatureType faalde: {}. Geef oorspronkelijke fout door.".format(e))
        _show_response_prefix(first_response, maxlen=220)
        raise WfsServerException("DescribeFeatureType faalde en request kon niet worden hersteld.")

    if not candidates:
        _show_response_prefix(first_response, maxlen=220)
        raise WfsServerException("Geen geometry-attribuut gevonden via DescribeFeatureType.")

    original_cql = original_params.get("cql_filter", "")
    for cand in candidates:
        new_params = dict(original_params)
        new_params["cql_filter"] = _replace_geom_attr_in_cql(original_cql, cand)
        print("[INFO] Retry met geometry-attribuut '{}': {}".format(cand, new_params['cql_filter'][:180]))
        rr = requests.post(base_url, data=new_params, timeout=timeout, headers=headers) \
             if _should_use_post(base_url, new_params) \
             else requests.get(base_url, params=new_params, timeout=timeout, headers=headers)

        if rr.status_code == 200 and not _is_ows_exception(rr.text or ""):
            try:
                data = rr.json()
                _set_cached_geom_attr(base_url, typename, cand)
                return data, version
            except ValueError:
                _show_response_prefix(rr)
                continue
        else:
            _show_response_prefix(rr, maxlen=200)

    _show_response_prefix(first_response, maxlen=220)
    raise WfsServerException("Alle geometry-retries faalden.")

def iter_wfs_batches(base_url, typename, bbox=None, version="2.0.0",
                     srs="EPSG:28992", page_size=5000,
                     output_format="application/json", timeout=180,
                     headers=None, extra_params=None, *,
                     max_start_index=None, sort_by_11: str = "fid"):
    """
    Generator die batches (feature-lijsten) yield uit een WFS bron.

    - WFS 2.0.0: automatische paginatie met startIndex/count
    - WFS 1.1.0: iteratieve paginatie via vendor-param startIndex + maxFeatures (GeoServer)
                  en (default) sortBy=fid voor stabiel resultaat.
    - bbox is optioneel; bij GeoServer geldt vaak: niet tegelijk met cql_filter gebruiken.
    - Bij 'Illegal property name' wordt automatisch DescribeFeatureType gedaan
      en nogmaals geprobeerd met de juiste geometry-naam (en gecached voor vervolg).
    """
    start = 0
    total = 0
    seen_ids = set()  # dup-guard bij instabiele volgorde

    # (optioneel) probe met resultType=hits
    try:
        probe = {
            "service": "WFS",
            "request": "GetFeature",
            "version": version,
            ("typeNames" if version == "2.0.0" else "typeName"): typename,
            "srsName": srs,
            "resultType": "hits",
        }
        if bbox is not None:
            probe["bbox"] = "{},{},{},{}".format(bbox["xmin"], bbox["ymin"], bbox["xmax"], bbox["ymax"])
        if extra_params:
            probe.update(extra_params)
        pr = requests.post(base_url, data=probe, timeout=timeout, headers=headers) \
             if _should_use_post(base_url, probe) \
             else requests.get(base_url, params=probe, timeout=timeout, headers=headers)
        if pr.status_code == 200 and "numberMatched" in pr.text:
            print("[WFS] Probe (resultType=hits) response:")
            _show_response_prefix(pr, maxlen=260)
    except Exception:
        pass

    while True:
        data, ver = fetch_page(
            base_url=base_url,
            typename=typename,
            bbox=bbox,
            start_index=start,
            count=page_size,
            version=version,
            srs=srs,
            output_format=output_format,
            timeout=timeout,
            headers=headers,
            extra_params=extra_params,
            sort_by_11=sort_by_11,
        )

        feats = data.get("features", [])
        n = len(feats)
        print("[WFS] startIndex={} requested={} returned={}".format(start, page_size, n))
        if not feats:
            break

        if ver == "1.1.0":
            # dup-guard o.b.v. fid property (indien aanwezig)
            new_feats, dups = [], 0
            for f in feats:
                fid_val = (f.get("properties") or {}).get("fid")
                if fid_val is None or fid_val not in seen_ids:
                    if fid_val is not None:
                        seen_ids.add(fid_val)
                    new_feats.append(f)
                else:
                    dups += 1
            if dups:
                print(f"[WFS] gedropte duplicates in page: {dups}")
            feats = new_feats
            n = len(feats)
            if not feats:
                break

        yield feats
        total += n

        start += n
        if n < page_size:
            break
        if (max_start_index is not None) and (start >= max_start_index):
            print(f"[WFS] max_start_index {max_start_index} bereikt. Stop batchen.")
            break
        time.sleep(0.5)

    print("[WFS] Totaal features ontvangen: {}".format(total))


# ─────────────────────────────────────────────────────────────────────────────
# CQL helpers
# ─────────────────────────────────────────────────────────────────────────────

def build_cql_intersects(geom_attr: str, geom_wkt: str, srs: str = "EPSG:28992") -> str:
    srid = srs.split(":")[1] if ":" in srs else srs
    safe_wkt = re.sub(r"\s+", " ", geom_wkt.strip())
    return "INTERSECTS({},SRID={};{})".format(geom_attr, srid, safe_wkt)
