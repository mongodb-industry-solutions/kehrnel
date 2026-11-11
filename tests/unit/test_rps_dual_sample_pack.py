from __future__ import annotations

import json
import re
import tomllib
from datetime import datetime, timezone
from pathlib import Path

import pytest

from kehrnel.engine.core.types import StrategyContext
from kehrnel.engine.domains.openehr.templates import (
    OptSemanticCatalogCompiler,
    TemplateParser,
    kehrnelValidator,
)
from kehrnel.engine.strategies.openehr.rps_dual.config import (
    build_coding_opts,
    build_flattener_config,
    normalize_config,
)
from kehrnel.engine.strategies.openehr.rps_dual.index_definition_builder import (
    build_search_index_definition_from_mappings,
)
from kehrnel.engine.strategies.openehr.rps_dual.ingest.flattener import (
    CompositionFlattener,
)
from kehrnel.engine.strategies.openehr.rps_dual.samples.build_heart_failure_demo import (
    HEART_FAILURE_CODES,
    SINCE,
    SNOMED_CT,
    build_semantic_projection_fields,
)
from kehrnel.engine.strategies.openehr.rps_dual.strategy import (
    MANIFEST,
    RPSDualStrategy,
)


STRATEGY_ROOT = (
    Path(__file__).resolve().parents[2]
    / "src"
    / "kehrnel"
    / "engine"
    / "strategies"
    / "openehr"
    / "rps_dual"
)
SAMPLES_ROOT = STRATEGY_ROOT / "samples" / "reference"
PROJECTION_MAPPINGS_PATH = SAMPLES_ROOT / "projection_mappings.json"
PACKAGED_SEARCH_INDEX_PATH = SAMPLES_ROOT / "search_index.definition.json"
PACKAGED_ACTIVATION_CONFIG_PATH = SAMPLES_ROOT / "activation.config.json"
PACKAGED_MANIFEST_PATH = SAMPLES_ROOT / "manifest.json"
HEART_FAILURE_ENVELOPES_PATH = (
    SAMPLES_ROOT / "envelopes" / "sample_laboratory_v0_4_heart_failure.ndjson"
)
HEART_FAILURE_CORRELATION_FIXTURE_PATH = (
    SAMPLES_ROOT / "adversarial" / "split_evaluation_correlation.envelope.json"
)
BUNDLED_SEARCH_INDEX_PATH = STRATEGY_ROOT / "bundles" / "searchIndex" / "searchIndex.json"
BUNDLED_CODES_PATH = STRATEGY_ROOT / "bundles" / "dictionaries" / "_codes.json"
SHORTCUTS_PATH = STRATEGY_ROOT / "bundles" / "shortcuts" / "shortcuts.json"
DEFAULTS_PATH = STRATEGY_ROOT / "defaults.json"
QUERIES_ROOT = SAMPLES_ROOT / "queries"
PYPROJECT_PATH = Path(__file__).resolve().parents[2] / "pyproject.toml"


_MISSING = object()


class _FakeCollection:
    def __init__(self, docs=None):
        self.docs = docs or {}

    async def find_one(self, flt=None, projection=None):
        if flt and "_id" in flt:
            return self.docs.get(flt["_id"])
        return next(iter(self.docs.values())) if self.docs else None

    def find(self, flt=None):
        class _Cursor:
            def __init__(self, docs):
                self._docs = docs

            def limit(self, _n):
                return self

            async def to_list(self, length=None):
                return self._docs[:length] if length is not None else self._docs

        return _Cursor(list(self.docs.values()))


class _FakeDb(dict):
    def __getitem__(self, name):
        return super().__getitem__(name)


class _FakeStorage:
    def __init__(self, db):
        self.db = db

    async def find_one(self, collection, flt):
        return await self.db[collection].find_one(flt)


def _lookup_path(value, dotted_path: str):
    current = value
    for part in dotted_path.split(".") if dotted_path else ():
        if not isinstance(current, dict) or part not in current:
            return _MISSING
        current = current[part]
    return current


def _eval_mongo_expr(expr, document: dict, variables: dict | None = None):
    """Evaluate the focused aggregation-expression subset emitted by the compiler.

    This is intentionally a test oracle rather than an alternate query
    implementation: it evaluates the actual post-``$search`` ``$expr`` emitted
    by ``SearchPipelineBuilder`` against deterministic sidecar documents.
    """
    variables = variables or {}

    if isinstance(expr, str):
        if expr.startswith("$$"):
            variable_ref = expr[2:]
            name, _, path = variable_ref.partition(".")
            return _lookup_path(variables.get(name, _MISSING), path)
        if expr.startswith("$"):
            return _lookup_path(document, expr[1:])
        return expr
    if isinstance(expr, list):
        return [_eval_mongo_expr(item, document, variables) for item in expr]
    if not isinstance(expr, dict):
        return expr
    if len(expr) != 1 or not next(iter(expr)).startswith("$"):
        return {
            key: _eval_mongo_expr(value, document, variables)
            for key, value in expr.items()
        }

    operator, payload = next(iter(expr.items()))

    if operator == "$let":
        scoped = dict(variables)
        # MongoDB evaluates each variable declaration against the outer scope.
        scoped.update(
            {
                name: _eval_mongo_expr(value, document, variables)
                for name, value in payload.get("vars", {}).items()
            }
        )
        return _eval_mongo_expr(payload["in"], document, scoped)
    if operator == "$cond":
        if isinstance(payload, list):
            predicate, on_true, on_false = payload
        else:
            predicate = payload["if"]
            on_true = payload["then"]
            on_false = payload["else"]
        branch = on_true if _eval_mongo_expr(predicate, document, variables) else on_false
        return _eval_mongo_expr(branch, document, variables)
    if operator == "$ifNull":
        values = list(payload)
        for item in values[:-1]:
            resolved = _eval_mongo_expr(item, document, variables)
            if resolved is not None and resolved is not _MISSING:
                return resolved
        return _eval_mongo_expr(values[-1], document, variables)
    if operator in {"$eq", "$ne", "$gt", "$gte", "$lt", "$lte"}:
        left, right = (
            _eval_mongo_expr(item, document, variables) for item in payload
        )
        if left is _MISSING or right is _MISSING:
            return operator == "$ne" and left is not right
        if isinstance(left, datetime) and isinstance(right, datetime):
            if left.tzinfo is not None:
                left = left.astimezone(timezone.utc).replace(tzinfo=None)
            if right.tzinfo is not None:
                right = right.astimezone(timezone.utc).replace(tzinfo=None)
        comparators = {
            "$eq": lambda: left == right,
            "$ne": lambda: left != right,
            "$gt": lambda: left > right,
            "$gte": lambda: left >= right,
            "$lt": lambda: left < right,
            "$lte": lambda: left <= right,
        }
        return comparators[operator]()
    if operator == "$and":
        return all(_eval_mongo_expr(item, document, variables) for item in payload)
    if operator == "$or":
        return any(_eval_mongo_expr(item, document, variables) for item in payload)
    if operator == "$type":
        value = _eval_mongo_expr(payload, document, variables)
        if value is _MISSING:
            return "missing"
        if isinstance(value, str):
            return "string"
        if isinstance(value, list):
            return "array"
        if isinstance(value, dict):
            return "object"
        if isinstance(value, datetime):
            return "date"
        if value is None:
            return "null"
        return type(value).__name__
    if operator == "$isArray":
        return isinstance(_eval_mongo_expr(payload, document, variables), list)
    if operator == "$size":
        return len(_eval_mongo_expr(payload, document, variables))
    if operator == "$split":
        value, separator = (
            _eval_mongo_expr(item, document, variables) for item in payload
        )
        return value.split(separator)
    if operator == "$indexOfArray":
        values, target = (
            _eval_mongo_expr(item, document, variables) for item in payload[:2]
        )
        try:
            return values.index(target)
        except ValueError:
            return -1
    if operator == "$slice":
        values, start, count = (
            _eval_mongo_expr(item, document, variables) for item in payload
        )
        return values[start : start + count]
    if operator == "$range":
        args = [_eval_mongo_expr(item, document, variables) for item in payload]
        return list(range(*args))
    if operator in {"$map", "$filter"}:
        values = _eval_mongo_expr(payload["input"], document, variables)
        variable_name = payload.get("as", "this")
        results = []
        for value in values:
            scoped = {**variables, variable_name: value}
            if operator == "$map":
                results.append(_eval_mongo_expr(payload["in"], document, scoped))
            elif _eval_mongo_expr(payload["cond"], document, scoped):
                results.append(value)
        return results
    if operator == "$anyElementTrue":
        return any(_eval_mongo_expr(payload, document, variables))
    if operator == "$regexMatch":
        value = _eval_mongo_expr(payload["input"], document, variables)
        pattern = _eval_mongo_expr(payload["regex"], document, variables)
        return bool(re.search(pattern, value))
    if operator == "$toString":
        return str(_eval_mongo_expr(payload, document, variables))
    if operator == "$concat":
        return "".join(
            str(_eval_mongo_expr(item, document, variables)) for item in payload
        )
    if operator == "$reduce":
        result = _eval_mongo_expr(payload["initialValue"], document, variables)
        for item in _eval_mongo_expr(payload["input"], document, variables):
            result = _eval_mongo_expr(
                payload["in"],
                document,
                {**variables, "value": result, "this": item},
            )
        return result

    raise AssertionError(f"Unsupported aggregation expression in focused evaluator: {operator}")


def _atlas_index_field_type(definition: dict, dotted_path: str) -> str:
    fields = definition["mappings"]["fields"]
    field_definition = None
    for part in dotted_path.split("."):
        field_definition = fields[part]
        fields = field_definition.get("fields", {})
    assert field_definition is not None
    return field_definition["type"]


def _atlas_leaf_operators(value):
    if isinstance(value, dict):
        for key, child in value.items():
            if key in {"equals", "in", "range", "regex", "text"} and isinstance(child, dict):
                yield key, child
            yield from _atlas_leaf_operators(child)
    elif isinstance(value, list):
        for child in value:
            yield from _atlas_leaf_operators(child)


def _load_json(path: Path):
    return json.loads(path.read_text(encoding="utf-8"))


def _sample_runtime_config() -> dict:
    config = _load_json(DEFAULTS_PATH)
    activation = _load_json(PACKAGED_ACTIVATION_CONFIG_PATH)
    config.setdefault("transform", {}).update(activation.get("transform") or {})
    return config


def _bundled_codes_doc() -> dict:
    payload = _load_json(BUNDLED_CODES_PATH)
    if isinstance(payload, list):
        for item in payload:
            if isinstance(item, dict) and item.get("_id") == "ar_code":
                return item
        raise AssertionError("Packaged _codes bundle does not contain an ar_code document")
    return payload


async def _build_packaged_sample_db(*, include_adversarial: bool = False) -> _FakeDb:
    projection_mappings = _load_json(PROJECTION_MAPPINGS_PATH)
    strategy_cfg = normalize_config(
        {
            "transform": {
                "mappings": projection_mappings,
            }
        }
    )

    db = _FakeDb(
        {
            "_codes": _FakeCollection({"ar_code": _bundled_codes_doc()}),
            "_shortcuts": _FakeCollection({"shortcuts": _load_json(SHORTCUTS_PATH)}),
            "compositions_rps": _FakeCollection({}),
            "compositions_search": _FakeCollection({}),
        }
    )

    flattener = await CompositionFlattener.create(
        db=db,
        config=build_flattener_config(strategy_cfg),
        mappings_path="unused",
        mappings_content=projection_mappings,
        coding_opts=build_coding_opts(strategy_cfg),
    )

    for ndjson_path in sorted((SAMPLES_ROOT / "envelopes").glob("*.ndjson")):
        if ndjson_path.name == "all.ndjson":
            continue
        for line in ndjson_path.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            envelope = json.loads(line)
            base_doc, search_doc = flattener.transform_composition(envelope)
            db["compositions_rps"].docs[base_doc["comp_id"]] = base_doc
            if search_doc is not None:
                db["compositions_search"].docs[search_doc["_id"]] = search_doc

    if include_adversarial:
        envelope = _load_json(HEART_FAILURE_CORRELATION_FIXTURE_PATH)
        base_doc, search_doc = flattener.transform_composition(envelope)
        db["compositions_rps"].docs[base_doc["comp_id"]] = base_doc
        if search_doc is not None:
            db["compositions_search"].docs[search_doc["_id"]] = search_doc

    return db


@pytest.mark.asyncio
async def test_packaged_sample_envelopes_produce_search_sidecar_docs():
    strategy_cfg = normalize_config(
        {
            "transform": {
                "apply_shortcuts": False,
                "coding": {
                    "arcodes": {"strategy": "literal"},
                    "atcodes": {"strategy": "literal"},
                },
            }
        }
    )
    projection_mappings = _load_json(PROJECTION_MAPPINGS_PATH)
    flattener = await CompositionFlattener.create(
        db=None,
        config=build_flattener_config(strategy_cfg),
        mappings_path="unused",
        mappings_content=projection_mappings,
        coding_opts=build_coding_opts(strategy_cfg),
    )

    checked = 0
    for ndjson_path in sorted((SAMPLES_ROOT / "envelopes").glob("*.ndjson")):
        if ndjson_path.name == "all.ndjson":
            continue
        for line in ndjson_path.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            envelope = json.loads(line)
            checked += 1

            assert envelope["composition_version"] == "1"
            assert envelope["time_committed"]

            base_doc, search_doc = flattener.transform_composition(envelope)

            assert isinstance(base_doc.get("time_c"), datetime)
            assert search_doc is not None
            assert search_doc.get("sn")
            assert isinstance(search_doc.get("sort_time"), datetime)
            assert search_doc.get("tid") == envelope["template_id"]

    manifest = _load_json(PACKAGED_MANIFEST_PATH)
    expected_count = sum(item["composition_count"] for item in manifest["templates"])
    expected_count += manifest["heart_failure_demo"]["scenario_count"]
    assert checked == expected_count == 15


@pytest.mark.asyncio
async def test_default_activation_loads_packaged_semantic_projection_mappings():
    envelope = json.loads(
        HEART_FAILURE_ENVELOPES_PATH.read_text(encoding="utf-8").splitlines()[0]
    )
    db = _FakeDb(
        {
            "_codes": _FakeCollection({"ar_code": _bundled_codes_doc()}),
            "_shortcuts": _FakeCollection(
                {"shortcuts": _load_json(SHORTCUTS_PATH)}
            ),
        }
    )
    ctx = StrategyContext(
        environment_id="env-default-mappings",
        # An activation with no transform override must resolve the packaged
        # mapping file rather than disabling semantic projections.
        config={},
        adapters={"storage": _FakeStorage(db)},
        manifest=MANIFEST.model_copy(deep=True),
        meta={},
    )

    result = await RPSDualStrategy().transform(ctx, envelope)

    assert result.search is not None
    semantic_nodes = result.search["sn"]
    assert any(node.get("data", {}).get("ani") is not None for node in semantic_nodes)
    assert any(
        node.get("data", {}).get("v", {}).get("df", {}).get("cs")
        in HEART_FAILURE_CODES
        for node in semantic_nodes
    )
    assert any(
        node.get("data", {})
        .get("v", {})
        .get("df", {})
        .get("tid", {})
        .get("v")
        == SNOMED_CT
        for node in semantic_nodes
    )
    assert any(
        isinstance(node.get("data", {}).get("v", {}).get("v"), datetime)
        for node in semantic_nodes
    )


@pytest.mark.asyncio
async def test_packaged_activation_config_points_to_reference_projection_mappings():
    activation_cfg = _load_json(PACKAGED_ACTIVATION_CONFIG_PATH)
    assert activation_cfg == {
        "transform": {
            "mappings": "file://samples/reference/projection_mappings.json",
        }
    }


@pytest.mark.asyncio
async def test_packaged_search_index_definition_matches_generated_definition():
    projection_mappings = _load_json(PROJECTION_MAPPINGS_PATH)
    shortcuts = _load_json(SHORTCUTS_PATH)

    generated = await build_search_index_definition_from_mappings(
        normalize_config({}),
        projection_mappings,
        shortcuts=shortcuts,
    )

    packaged_definition = _load_json(PACKAGED_SEARCH_INDEX_PATH)
    bundled_definition = _load_json(BUNDLED_SEARCH_INDEX_PATH)

    assert generated["warnings"] == []
    assert generated["definition"] == packaged_definition
    assert packaged_definition == bundled_definition


def test_heart_failure_projection_paths_are_derived_from_the_packaged_opt():
    projection_mappings = _load_json(PROJECTION_MAPPINGS_PATH)
    laboratory = next(
        item
        for item in projection_mappings["templates"]
        if item["templateId"] == "sample_laboratory_v0.4"
    )
    semantic_names = {
        "Diagnosis archetype anchor",
        "Diagnosis (Code)",
        "Diagnosis (Terminology identifier)",
        "Date/time clinically recognised (Value)",
    }

    actual = [field for field in laboratory["fields"] if field["name"] in semantic_names]

    assert actual == build_semantic_projection_fields()


def test_heart_failure_projection_is_bound_to_the_packaged_opt_catalog():
    projection_mappings = _load_json(PROJECTION_MAPPINGS_PATH)
    laboratory = next(
        item
        for item in projection_mappings["templates"]
        if item["templateId"] == "sample_laboratory_v0.4"
    )
    catalog = OptSemanticCatalogCompiler.from_path(
        SAMPLES_ROOT / "templates" / "sample_laboratory_v0_4.opt"
    ).compile()

    assert laboratory["provenance"] == {
        "templateSourceDigest": catalog.template.source_digest,
        "catalogDigest": catalog.catalog_digest,
    }


def test_runnable_heart_failure_compositions_validate_against_the_packaged_opt():
    template = TemplateParser(
        SAMPLES_ROOT / "templates" / "sample_laboratory_v0_4.opt"
    )
    validator = kehrnelValidator(template)
    canonical_dir = (
        SAMPLES_ROOT / "canonical" / "sample_laboratory_v0_4_heart_failure"
    )

    validated = 0
    for sample in sorted(canonical_dir.glob("*.json")):
        assert validator.validate(_load_json(sample)) == [], sample.name
        validated += 1

    assert validated == 5


@pytest.mark.asyncio
async def test_heart_failure_demo_sidecars_enforce_terminology_date_and_evaluation_correlation():
    db = await _build_packaged_sample_db()
    envelopes = [
        json.loads(line)
        for line in HEART_FAILURE_ENVELOPES_PATH.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    correlation_fixture = _load_json(HEART_FAILURE_CORRELATION_FIXTURE_PATH)
    cutoff = datetime.fromisoformat(SINCE.replace("Z", "+00:00")).replace(tzinfo=None)

    def nested(node: dict, *path: str):
        current = node
        for part in path:
            if not isinstance(current, dict):
                return None
            current = current.get(part)
        return current

    def matches(search_doc: dict) -> bool:
        nodes = search_doc["sn"]
        anchors = [node for node in nodes if nested(node, "data", "ani") is not None]
        for anchor in anchors:
            anchor_path = anchor["p"]
            anchor_pi = anchor["pi"]
            correlated = [
                node
                for node in nodes
                if node.get("p", "").endswith(f":{anchor_path}")
                and node.get("pi", [])[-len(anchor_pi) :] == anchor_pi
            ]
            coded_nodes = [
                node
                for node in correlated
                if nested(node, "data", "v", "df", "cs") in HEART_FAILURE_CODES
            ]
            coded_in_snomed = any(
                any(
                    nested(term, "data", "v", "df", "tid", "v") == SNOMED_CT
                    and term.get("p") == coded.get("p")
                    and term.get("pi") == coded.get("pi")
                    for term in correlated
                )
                for coded in coded_nodes
            )
            recognised_in_period = any(
                isinstance(nested(node, "data", "v", "v"), datetime)
                and nested(node, "data", "v", "v") >= cutoff
                for node in correlated
            )
            if coded_in_snomed and recognised_in_period:
                return True
        return False

    observed = {
        envelope["demo_case"]: matches(db["compositions_search"].docs[envelope["_id"]])
        for envelope in envelopes
    }
    expected = {
        envelope["demo_case"]: envelope["expected_heart_failure_since_2025"]
        for envelope in envelopes
    }

    projection_mappings = _load_json(PROJECTION_MAPPINGS_PATH)
    strategy_cfg = normalize_config(
        {
            "transform": {
                "apply_shortcuts": False,
                "coding": {
                    "arcodes": {"strategy": "literal"},
                    "atcodes": {"strategy": "literal"},
                },
            }
        }
    )
    adversarial_flattener = await CompositionFlattener.create(
        db=None,
        config=build_flattener_config(strategy_cfg),
        mappings_path="unused",
        mappings_content=projection_mappings,
        coding_opts=build_coding_opts(strategy_cfg),
    )
    _, adversarial_search = adversarial_flattener.transform_composition(
        correlation_fixture
    )
    assert adversarial_search is not None

    def unshortened_matches(search_doc: dict) -> bool:
        nodes = search_doc["sn"]
        anchors = [
            node
            for node in nodes
            if nested(node, "data", "archetype_node_id") == (
                "openEHR-EHR-EVALUATION.problem_diagnosis.v1"
            )
        ]
        for anchor in anchors:
            anchor_pi = anchor["pi"]
            correlated = [
                node
                for node in nodes
                if node.get("p", "").endswith(f":{anchor['p']}")
                and node.get("pi", [])[-len(anchor_pi) :] == anchor_pi
            ]
            has_code = any(
                nested(node, "data", "value", "defining_code", "code_string")
                in HEART_FAILURE_CODES
                for node in correlated
            )
            has_terminology = any(
                nested(
                    node,
                    "data",
                    "value",
                    "defining_code",
                    "terminology_id",
                    "value",
                )
                == SNOMED_CT
                for node in correlated
            )
            has_date = any(
                isinstance(nested(node, "data", "value", "value"), datetime)
                and nested(node, "data", "value", "value") >= cutoff
                for node in correlated
            )
            if has_code and has_terminology and has_date:
                return True
        return False

    observed[correlation_fixture["demo_case"]] = unshortened_matches(
        adversarial_search
    )
    expected[correlation_fixture["demo_case"]] = False

    assert observed == expected
    assert observed["split-evaluation-correlation-negative"] is False


def test_heart_failure_search_index_covers_all_projected_query_facets():
    definition = _load_json(PACKAGED_SEARCH_INDEX_PATH)
    data_fields = definition["mappings"]["fields"]["sn"]["fields"]["data"][
        "fields"
    ]

    assert data_fields["ani"] == {"type": "number"}
    assert data_fields["v"]["fields"]["df"]["fields"]["cs"] == {"type": "token"}
    assert data_fields["v"]["fields"]["df"]["fields"]["tid"]["fields"][
        "v"
    ] == {"type": "token"}
    assert data_fields["v"]["fields"]["v"] == {"type": "date"}


@pytest.mark.asyncio
async def test_heart_failure_compiled_search_operators_match_bundled_index_types():
    db = await _build_packaged_sample_db()
    ctx = StrategyContext(
        environment_id="env-heart-failure-search-contract",
        config=_sample_runtime_config(),
        adapters={"storage": _FakeStorage(db)},
        manifest=MANIFEST.model_copy(deep=True),
        meta={},
    )
    query = (QUERIES_ROOT / "cross_patient_heart_failure_since_2025.aql").read_text(
        encoding="utf-8"
    )

    plan = await RPSDualStrategy().compile_query(
        ctx,
        "openEHR",
        {
            "raw_aql": query,
            "debug": True,
            "require_exact_path_correlation": True,
        },
    )

    search = plan.plan["pipeline"][0]["$search"]
    index_definition = _load_json(BUNDLED_SEARCH_INDEX_PATH)
    operators = list(_atlas_leaf_operators(search))
    allowed_types = {
        "equals": {"boolean", "date", "number", "objectId", "token", "uuid"},
        "in": {"boolean", "date", "number", "objectId", "token", "uuid"},
        "range": {"date", "number"},
        "regex": {"string"},
        "text": {"string"},
    }

    for operator, body in operators:
        paths = body["path"] if isinstance(body["path"], list) else [body["path"]]
        for path in paths:
            field_type = _atlas_index_field_type(index_definition, path)
            assert field_type in allowed_types[operator], (
                f"Atlas Search {operator} cannot target {path!r}, which is mapped as "
                f"{field_type!r}"
            )

    exact_values = {
        (body["path"], body["value"])
        for operator, body in operators
        if operator == "equals"
    }
    assert ("sn.data.v.df.tid.v", SNOMED_CT) in exact_values
    exact_sets = {
        body["path"]: set(body["value"])
        for operator, body in operators
        if operator == "in"
    }
    assert exact_sets["sn.data.v.df.cs"] == set(HEART_FAILURE_CODES)
    assert not any(
        operator in {"regex", "text"}
        and _atlas_index_field_type(index_definition, body["path"]) == "token"
        for operator, body in operators
    )
    assert search["index"] == "search_nodes_index"


@pytest.mark.asyncio
async def test_compiled_heart_failure_exact_correlation_runs_against_demo_fixtures():
    db = await _build_packaged_sample_db(include_adversarial=True)
    ctx = StrategyContext(
        environment_id="env-heart-failure-exact-correlation",
        config=_sample_runtime_config(),
        adapters={"storage": _FakeStorage(db)},
        manifest=MANIFEST.model_copy(deep=True),
        meta={},
    )
    query = (QUERIES_ROOT / "cross_patient_heart_failure_since_2025.aql").read_text(
        encoding="utf-8"
    )

    plan = await RPSDualStrategy().compile_query(
        ctx,
        "openEHR",
        {
            "raw_aql": query,
            "debug": True,
            "require_exact_path_correlation": True,
        },
    )
    exact_expressions = [
        stage["$match"]["$expr"]
        for stage in plan.plan["pipeline"][1:]
        if "$expr" in stage.get("$match", {})
    ]
    assert len(exact_expressions) == 1

    envelopes = [
        json.loads(line)
        for line in HEART_FAILURE_ENVELOPES_PATH.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    envelopes.append(_load_json(HEART_FAILURE_CORRELATION_FIXTURE_PATH))

    observed = {
        envelope["demo_case"]: all(
            _eval_mongo_expr(
                expression,
                db["compositions_search"].docs[envelope["_id"]],
            )
            for expression in exact_expressions
        )
        for envelope in envelopes
    }
    expected = {
        envelope["demo_case"]: envelope["expected_heart_failure_since_2025"]
        for envelope in envelopes
    }

    assert observed == expected
    assert observed["split-evaluation-correlation-negative"] is False


def test_sample_aql_queries_are_included_in_package_data_configuration():
    package_data = tomllib.loads(PYPROJECT_PATH.read_text(encoding="utf-8"))["tool"][
        "setuptools"
    ]["package-data"]["*"]

    assert "**/*.aql" in package_data


@pytest.mark.asyncio
async def test_packaged_sample_aql_queries_compile_against_packaged_dataset():
    db = await _build_packaged_sample_db()
    ctx = StrategyContext(
        environment_id="env-samples",
        config=_sample_runtime_config(),
        adapters={"storage": _FakeStorage(db)},
        manifest=MANIFEST.model_copy(deep=True),
        meta={},
    )
    strategy = RPSDualStrategy()

    expectations = {
        "patient_laboratory_by_ehr.aql": {"stage0": "$match", "scope": "patient"},
        "cross_patient_laboratory_by_performing_centre.aql": {
            "stage0": "$search",
            "scope": "cross_patient",
        },
        "cross_patient_immunization_by_template.aql": {
            "stage0": "$match",
            "scope": "cross_patient",
        },
        "cross_patient_immunization_by_publishing_centre.aql": {
            "stage0": "$search",
            "scope": "cross_patient",
        },
        "cross_patient_heart_failure_since_2025.aql": {
            "stage0": "$search",
            "scope": "cross_patient",
        },
    }

    compiled = 0
    for query_path in sorted(QUERIES_ROOT.glob("*.aql")):
        query_text = query_path.read_text(encoding="utf-8").strip()
        assert query_text, f"Sample query {query_path.name} must not be empty"

        request = {
            "raw_aql": query_text,
            "debug": True,
        }
        if query_path.name == "cross_patient_heart_failure_since_2025.aql":
            request["require_exact_path_correlation"] = True

        plan = await strategy.compile_query(
            ctx,
            "openEHR",
            request,
        )

        pipeline = plan.plan.get("pipeline", [])
        assert pipeline, f"Sample query {query_path.name} did not produce a pipeline"

        expected = expectations[query_path.name]
        stage0 = next(iter(pipeline[0]))
        assert stage0 == expected["stage0"]
        assert plan.explain["scope"] == expected["scope"]
        if query_path.name == "cross_patient_heart_failure_since_2025.aql":
            assert plan.explain["builder"]["exact_path_correlation_required"] is True
            assert any("$expr" in stage.get("$match", {}) for stage in pipeline[1:])
            assert pipeline[-2] == {
                "$sort": {"clinicalStartTime": -1, "compositionUid": 1}
            }
            assert pipeline[-1] == {"$limit": 50}
        compiled += 1

    assert compiled == len(expectations)
