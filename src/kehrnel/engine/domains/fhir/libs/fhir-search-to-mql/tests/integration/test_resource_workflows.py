"""
Workflow integration tests covering the lowest-coverage modules.

Targets, with the previously-uncovered code paths each test exercises:

- ``fhir_search_to_mql.__init__``        — package import + public exports
- ``ResourceDenormalizer``               — denormalize_with_config, _from_file,
                                            _from_folder, denormalize_field,
                                            validate, get_denormalization_rules,
                                            denormalize_from_mongodb (real Mongo)
- ``FileHandler``                        — read_resource, write_resource,
                                            read_bundle, write_bundle,
                                            process_folder, batch_write,
                                            validate_json, get_file_stats
- ``MongoDBHandler``                     — read_resources, write_resources,
                                            update_search_fields, batch_process,
                                            get_collection_stats,
                                            ensure_indexes, remove_search_fields,
                                            copy_collection
- ``ConfigLoader``                       — error paths, version mismatch,
                                            list_resources, has_config, reload,
                                            get_search_parameters,
                                            get_denormalization_rules
- ``CompositeConverter``                 — handler paths now wired into
                                            FHIRSearchConverter

These tests use the real configs in ``configs/`` and the real
``localhost:27017`` MongoDB (the user-confirmed runtime).  The MongoDB-bound
class is automatically skipped if Mongo is unreachable.
"""

import json
import os
import pytest
from pathlib import Path
from typing import Any, Dict, List

from fhir_search_to_mql.core.config_loader import ConfigLoader
from fhir_search_to_mql.core.exceptions import (
    ConfigurationError,
    DenormalizationError,
    MissingConfigurationError,
    ValidationError)
from fhir_search_to_mql.denormalizer import ResourceDenormalizer
from fhir_search_to_mql.denormalizer.file_handler import FileHandler
from fhir_search_to_mql.denormalizer.mongodb_handler import MongoDBHandler


pytestmark = pytest.mark.integration


# ===========================================================================
# 0) Package public surface
# ===========================================================================

class TestPackageImports:
    """Cover the package ``__init__`` so the public surface is exercised."""

    def test_top_level_imports(self):
        import fhir_search_to_mql as pkg
        assert pkg.__version__
        assert pkg.__author__
        for name in (
            "ConfigLoader",
            "ResourceDenormalizer",
            "FHIRSearchConverter",
            "QueryParser",
            "MQLBuilder",
            "FHIRSearchToMQLError",
            "ConfigurationError",
            "ValidationError",
            "ConversionError",
            "ParsingError",
            "ResourceNotInCompartmentError",
            "UnsupportedParameterError",
            "InvalidModifierError",
            "InvalidPrefixError",
            "MissingConfigurationError",
            "DenormalizationError"):
            assert hasattr(pkg, name), f"public export missing: {name}"

    def test_exception_hierarchy(self):
        from fhir_search_to_mql import (
            FHIRSearchToMQLError,
            ConfigurationError,
            ValidationError,
            ConversionError,
            ParsingError,
            DenormalizationError)
        for cls in (
            ConfigurationError,
            ValidationError,
            ConversionError,
            ParsingError,
            DenormalizationError):
            assert issubclass(cls, FHIRSearchToMQLError)


# ===========================================================================
# 1) ConfigLoader edge cases
# ===========================================================================

class TestConfigLoaderEdgeCases:
    """Exercise the error and helper paths of ConfigLoader."""

    def test_init_defaults_to_bundled_configs(self):
        """
        ``ConfigLoader()`` (no args) now resolves the YAML configs
        bundled inside the package via ``importlib.resources``. The
        previous behavior of raising when neither ``config_path`` nor
        ``config_dir`` is provided was a footgun for downstream
        projects installing this library by pip — they had to know
        the on-disk layout to use the API. Bundled defaults make the
        library plug-and-play.
        """
        loader = ConfigLoader()
        resources = loader.list_resources()
        # All bundled resources must be available out of the box.
        assert {"Patient", "Observation", "Appointment", "Organization", "Location"}.issubset(
            set(resources)
        )

    def test_init_with_missing_dir(self):
        with pytest.raises(ConfigurationError):
            ConfigLoader(config_dir="this/path/does/not/exist")

    def test_init_with_file_as_dir(self, tmp_path):
        f = tmp_path / "not_a_dir.yaml"
        f.write_text("resource: X\nfhir_version: R5\n", encoding="utf-8")
        with pytest.raises(ConfigurationError):
            ConfigLoader(config_dir=str(f))

    def test_init_empty_dir(self, tmp_path):
        with pytest.raises(ConfigurationError):
            ConfigLoader(config_dir=str(tmp_path))

    def test_invalid_yaml(self, tmp_path):
        bad = tmp_path / "Bad.yaml"
        bad.write_text("resource: Bad\n  : oops :\n  bad: [unterminated", encoding="utf-8")
        # _load_all_configs swallows individual file errors with a warning,
        # but if every file is invalid we end up with nothing cached.
        loader = ConfigLoader(config_dir=str(tmp_path))
        assert loader.list_resources() == []

    def test_missing_resource_field(self, tmp_path):
        bad = tmp_path / "Missing.yaml"
        bad.write_text("fhir_version: R5\nsearch_parameters:\n  foo:\n    type: token\n    fields: []\n",
                       encoding="utf-8")
        loader = ConfigLoader(config_dir=str(tmp_path))
        assert loader.list_resources() == []

    def test_invalid_fhir_version(self, tmp_path):
        bad = tmp_path / "X.yaml"
        bad.write_text(
            "resource: X\nfhir_version: R99\n"
            "search_parameters:\n  foo:\n    type: token\n    fields: []\n",
            encoding="utf-8")
        loader = ConfigLoader(config_dir=str(tmp_path))
        assert loader.list_resources() == []

    def test_search_parameters_not_dict(self, tmp_path):
        bad = tmp_path / "X.yaml"
        bad.write_text(
            "resource: X\nfhir_version: R5\nsearch_parameters: not-a-dict\n",
            encoding="utf-8")
        loader = ConfigLoader(config_dir=str(tmp_path))
        assert loader.list_resources() == []

    def test_param_missing_type(self, tmp_path):
        bad = tmp_path / "X.yaml"
        bad.write_text(
            "resource: X\nfhir_version: R5\n"
            "search_parameters:\n  foo:\n    fields: []\n",
            encoding="utf-8")
        loader = ConfigLoader(config_dir=str(tmp_path))
        assert loader.list_resources() == []

    def test_param_invalid_type(self, tmp_path):
        bad = tmp_path / "X.yaml"
        bad.write_text(
            "resource: X\nfhir_version: R5\n"
            "search_parameters:\n  foo:\n    type: bogus\n    fields: []\n",
            encoding="utf-8")
        loader = ConfigLoader(config_dir=str(tmp_path))
        assert loader.list_resources() == []

    def test_composite_missing_components(self, tmp_path):
        bad = tmp_path / "X.yaml"
        bad.write_text(
            "resource: X\nfhir_version: R5\n"
            "search_parameters:\n  foo:\n    type: composite\n",
            encoding="utf-8")
        loader = ConfigLoader(config_dir=str(tmp_path))
        assert loader.list_resources() == []

    def test_composite_components_must_be_list(self, tmp_path):
        bad = tmp_path / "X.yaml"
        bad.write_text(
            "resource: X\nfhir_version: R5\n"
            "search_parameters:\n  foo:\n    type: composite\n    components: not-a-list\n",
            encoding="utf-8")
        loader = ConfigLoader(config_dir=str(tmp_path))
        assert loader.list_resources() == []

    def test_composite_component_missing_type(self, tmp_path):
        bad = tmp_path / "X.yaml"
        bad.write_text(
            "resource: X\nfhir_version: R5\n"
            "search_parameters:\n"
            "  foo:\n    type: composite\n"
            "    components:\n      - name: a\n",
            encoding="utf-8")
        loader = ConfigLoader(config_dir=str(tmp_path))
        assert loader.list_resources() == []

    def test_fields_must_be_list_or_dict(self, tmp_path):
        bad = tmp_path / "X.yaml"
        bad.write_text(
            "resource: X\nfhir_version: R5\n"
            "search_parameters:\n  foo:\n    type: token\n    fields: invalid-string\n",
            encoding="utf-8")
        loader = ConfigLoader(config_dir=str(tmp_path))
        assert loader.list_resources() == []

    def test_denormalization_must_be_dict(self, tmp_path):
        bad = tmp_path / "X.yaml"
        bad.write_text(
            "resource: X\nfhir_version: R5\n"
            "search_parameters:\n  foo:\n    type: token\n    fields: []\n"
            "denormalization: not-a-dict\n",
            encoding="utf-8")
        loader = ConfigLoader(config_dir=str(tmp_path))
        assert loader.list_resources() == []

    def test_denormalization_missing_extractor(self, tmp_path):
        bad = tmp_path / "X.yaml"
        bad.write_text(
            "resource: X\nfhir_version: R5\n"
            "search_parameters:\n  foo:\n    type: token\n    fields: []\n"
            "denormalization:\n  field1:\n    target: _search\n",
            encoding="utf-8")
        loader = ConfigLoader(config_dir=str(tmp_path))
        assert loader.list_resources() == []

    def test_denormalization_field_mappings_must_be_list(self, tmp_path):
        bad = tmp_path / "X.yaml"
        bad.write_text(
            "resource: X\nfhir_version: R5\n"
            "search_parameters:\n  foo:\n    type: token\n    fields: []\n"
            "denormalization:\n  field1:\n    extractor: HumanNameExtractor\n"
            "    field_mappings: not-a-list\n",
            encoding="utf-8")
        loader = ConfigLoader(config_dir=str(tmp_path))
        assert loader.list_resources() == []

    def test_denormalization_mapping_missing_target_field(self, tmp_path):
        bad = tmp_path / "X.yaml"
        bad.write_text(
            "resource: X\nfhir_version: R5\n"
            "search_parameters:\n  foo:\n    type: token\n    fields: []\n"
            "denormalization:\n  field1:\n    extractor: HumanNameExtractor\n"
            "    field_mappings:\n      - source_path: a.b\n",
            encoding="utf-8")
        loader = ConfigLoader(config_dir=str(tmp_path))
        assert loader.list_resources() == []

    def test_get_config_with_version_match(self):
        loader = ConfigLoader()
        cfg = loader.get_config("Patient", fhir_version="R5")
        assert cfg["resource"] == "Patient"

    def test_get_config_version_mismatch(self):
        loader = ConfigLoader()
        with pytest.raises(MissingConfigurationError):
            loader.get_config("Patient", fhir_version="R4")

    def test_unknown_resource(self):
        loader = ConfigLoader()
        with pytest.raises(MissingConfigurationError):
            loader.get_config("DoesNotExist")
        assert not loader.has_config("DoesNotExist")
        assert loader.has_config("Patient")

    def test_list_resources_and_helpers(self):
        loader = ConfigLoader()
        names = set(loader.list_resources())
        assert {"Patient", "Observation", "Appointment"} <= names
        assert "code" in loader.get_search_parameters("Observation")
        assert "name" in loader.get_search_parameters("Patient")
        rules = loader.get_denormalization_rules("Patient")
        assert "name" in rules

    def test_reload(self, tmp_path):
        cfg_dir = tmp_path / "cfg"
        cfg_dir.mkdir()
        f = cfg_dir / "Foo.yaml"
        f.write_text(
            "resource: Foo\nfhir_version: R5\n"
            "search_parameters:\n  bar:\n    type: token\n    fields:\n"
            "      - field: bar\n",
            encoding="utf-8")
        loader = ConfigLoader(config_dir=str(cfg_dir))
        assert loader.has_config("Foo")
        # Replace with different resource name
        f.write_text(
            "resource: Baz\nfhir_version: R5\n"
            "search_parameters:\n  bar:\n    type: token\n    fields:\n"
            "      - field: bar\n",
            encoding="utf-8")
        loader.reload()
        assert loader.has_config("Baz")
        assert not loader.has_config("Foo")

    def test_load_single_config(self, tmp_path):
        f = tmp_path / "Solo.yaml"
        f.write_text(
            "resource: Solo\nfhir_version: R5\n"
            "search_parameters:\n  bar:\n    type: token\n    fields:\n"
            "      - field: bar\n",
            encoding="utf-8")
        loader = ConfigLoader(config_path=str(f))
        assert loader.has_config("Solo")

    def test_load_single_file_not_found(self, tmp_path):
        with pytest.raises(ConfigurationError):
            ConfigLoader(config_path=str(tmp_path / "nope.yaml"))


# ===========================================================================
# 2) FileHandler — read/write/process_folder/batch/validate/get_file_stats
# ===========================================================================

class TestFileHandler:
    """Cover all FileHandler static methods (file_handler.py)."""

    @pytest.fixture
    def patient(self, sample_patient):
        return sample_patient

    @pytest.fixture
    def observation(self, sample_observation):
        return sample_observation

    @pytest.fixture
    def appointment(self, sample_appointment):
        return sample_appointment

    def test_read_write_resource(self, tmp_path, patient):
        path = tmp_path / "patient.json"
        FileHandler.write_resource(str(path), patient)
        assert path.exists()
        loaded = FileHandler.read_resource(str(path))
        assert loaded == patient

    def test_write_creates_parent_dirs(self, tmp_path, patient):
        path = tmp_path / "deep" / "nested" / "patient.json"
        FileHandler.write_resource(str(path), patient)
        assert path.exists()

    def test_read_missing_file_raises(self, tmp_path):
        with pytest.raises(DenormalizationError):
            FileHandler.read_resource(str(tmp_path / "nope.json"))

    def test_read_invalid_json_raises(self, tmp_path):
        path = tmp_path / "bad.json"
        path.write_text("{ this is :: not json }", encoding="utf-8")
        with pytest.raises(DenormalizationError):
            FileHandler.read_resource(str(path))

    def test_read_non_dict_raises(self, tmp_path):
        path = tmp_path / "list.json"
        path.write_text(json.dumps([1, 2, 3]), encoding="utf-8")
        with pytest.raises(DenormalizationError):
            FileHandler.read_resource(str(path))

    def test_read_write_bundle(self, tmp_path, patient, observation, appointment):
        path = tmp_path / "bundle.json"
        FileHandler.write_bundle(str(path), [patient, observation, appointment])
        loaded = FileHandler.read_bundle(str(path))
        assert len(loaded) == 3
        types = {r["resourceType"] for r in loaded}
        assert types == {"Patient", "Observation", "Appointment"}

    def test_read_bundle_invalid_type_raises(self, tmp_path, patient):
        path = tmp_path / "not-bundle.json"
        FileHandler.write_resource(str(path), patient)
        with pytest.raises(DenormalizationError):
            FileHandler.read_bundle(str(path))

    def test_process_folder_filters_by_resource_type(
        self, tmp_path, patient, observation, appointment
    ):
        for r in (patient, observation, appointment):
            FileHandler.write_resource(
                str(tmp_path / f"{r['resourceType']}_{r['id']}.json"), r
            )
        observations = FileHandler.process_folder(
            str(tmp_path), resource_type="Observation"
        )
        assert len(observations) == 1
        assert observations[0]["resourceType"] == "Observation"

    def test_process_folder_with_processor(self, tmp_path, patient):
        FileHandler.write_resource(str(tmp_path / "p.json"), patient)

        def stamp(resource):
            resource["meta"] = {"processed": True}
            return resource

        out = FileHandler.process_folder(str(tmp_path), processor=stamp)
        assert len(out) == 1
        assert out[0]["meta"]["processed"] is True

    def test_process_folder_recursive(
        self, tmp_path, patient, observation
    ):
        sub = tmp_path / "sub"
        sub.mkdir()
        FileHandler.write_resource(str(tmp_path / "p.json"), patient)
        FileHandler.write_resource(str(sub / "o.json"), observation)
        flat = FileHandler.process_folder(str(tmp_path), recursive=False)
        deep = FileHandler.process_folder(str(tmp_path), recursive=True)
        assert len(flat) == 1
        assert len(deep) == 2

    def test_process_folder_missing_dir(self, tmp_path):
        with pytest.raises(DenormalizationError):
            FileHandler.process_folder(str(tmp_path / "missing"))

    def test_process_folder_path_is_file(self, tmp_path, patient):
        f = tmp_path / "p.json"
        FileHandler.write_resource(str(f), patient)
        with pytest.raises(DenormalizationError):
            FileHandler.process_folder(str(f))

    def test_process_folder_skips_unreadable(
        self, tmp_path, patient
    ):
        FileHandler.write_resource(str(tmp_path / "good.json"), patient)
        (tmp_path / "bad.json").write_text("{ broken", encoding="utf-8")
        out = FileHandler.process_folder(str(tmp_path))
        assert len(out) == 1

    def test_batch_write(self, tmp_path, patient, observation, appointment):
        out_dir = tmp_path / "out"
        n = FileHandler.batch_write(
            [patient, observation, appointment], str(out_dir)
        )
        assert n == 3
        files = list(out_dir.glob("*.json"))
        assert len(files) == 3

    def test_batch_write_custom_template(self, tmp_path, patient):
        out_dir = tmp_path / "out2"
        n = FileHandler.batch_write(
            [patient, patient],
            str(out_dir),
            filename_template="r_{index:03d}.json")
        assert n == 2
        assert (out_dir / "r_000.json").exists()
        assert (out_dir / "r_001.json").exists()

    def test_validate_json(self, tmp_path):
        good = tmp_path / "good.json"
        good.write_text(json.dumps({"a": 1}), encoding="utf-8")
        bad = tmp_path / "bad.json"
        bad.write_text("not json", encoding="utf-8")
        assert FileHandler.validate_json(str(good)) is True
        assert FileHandler.validate_json(str(bad)) is False

    def test_get_file_stats(self, tmp_path, patient, observation, appointment):
        for r in (patient, observation, appointment):
            FileHandler.write_resource(
                str(tmp_path / f"{r['resourceType']}_{r['id']}.json"), r
            )
        stats = FileHandler.get_file_stats(str(tmp_path))
        assert stats["total_files"] == 3
        assert stats["total_size"] > 0
        assert stats["resource_types"] == {
            "Patient": 1,
            "Observation": 1,
            "Appointment": 1,
        }

    def test_get_file_stats_missing_dir(self, tmp_path):
        stats = FileHandler.get_file_stats(str(tmp_path / "missing"))
        assert stats["total_files"] == 0
        assert stats["resource_types"] == {}

    def test_get_file_stats_skips_invalid(self, tmp_path, patient):
        FileHandler.write_resource(str(tmp_path / "p.json"), patient)
        (tmp_path / "bad.json").write_text("not json", encoding="utf-8")
        stats = FileHandler.get_file_stats(str(tmp_path))
        # bad.json counts toward total_files (path glob), but resource_types
        # should only have valid resource types.
        assert stats["resource_types"] == {"Patient": 1}


# ===========================================================================
# 3) ResourceDenormalizer end-to-end (file/folder/field/mongo paths)
# ===========================================================================

class TestResourceDenormalizerWorkflows:
    """Cover ResourceDenormalizer's file/folder/field/mongo entry points."""

    @pytest.fixture(scope="class")
    def denormalizer(self):
        return ResourceDenormalizer()

    def test_default_denormalizer_uses_bundled_configs(self, sample_patient):
        """
        ``ResourceDenormalizer()`` now defaults to the bundled YAML
        configs (Patient/Observation/Appointment/Organization/Location).
        A Patient passed in without an explicit config_dir is fully
        denormalized as a result.
        """
        d = ResourceDenormalizer()
        out = d.denormalize(sample_patient)
        assert "_search" in out
        # Patient is in its own compartment, so _compartments.Patient
        # is precomputed too.
        assert "_compartments" in out
        # Original isn't mutated.
        assert "_search" not in sample_patient

    def test_denormalizer_with_unknown_resource_is_noop(self):
        """A resource type that has no bundled config returns unchanged."""
        d = ResourceDenormalizer()
        out = d.denormalize({"resourceType": "NotConfigured", "id": "x"})
        assert "_search" not in out

    def test_denormalize_missing_resourcetype(self, denormalizer):
        with pytest.raises(DenormalizationError):
            denormalizer.denormalize({"id": "no-type"})

    def test_denormalize_unknown_resource_type(self, denormalizer):
        out = denormalizer.denormalize(
            {"resourceType": "NotConfigured", "id": "x"}
        )
        assert out["id"] == "x"
        assert "_search" not in out

    def test_denormalize_with_config_no_rules(self, sample_patient):
        d = ResourceDenormalizer()
        out = d.denormalize_with_config(sample_patient, {"resource": "Patient"})
        # No denormalization rules in the supplied dict → no _search added
        assert "_search" not in out

    def test_denormalize_with_config(self, sample_patient, sample_patient_config):
        d = ResourceDenormalizer()
        out = d.denormalize_with_config(sample_patient, sample_patient_config)
        assert "_search" in out
        assert "smith" in out["_search"]["familyName_lower"]

    def test_denormalize_from_file(self, tmp_path, sample_observation, denormalizer):
        path = tmp_path / "o.json"
        FileHandler.write_resource(str(path), sample_observation)
        out = denormalizer.denormalize_from_file(str(path))
        assert "_search" in out
        assert "code_codes" in out["_search"]

    def test_denormalize_from_file_missing(self, tmp_path, denormalizer):
        with pytest.raises(DenormalizationError):
            denormalizer.denormalize_from_file(str(tmp_path / "missing.json"))

    def test_denormalize_from_file_invalid_json(self, tmp_path, denormalizer):
        bad = tmp_path / "bad.json"
        bad.write_text("{ broken", encoding="utf-8")
        with pytest.raises(DenormalizationError):
            denormalizer.denormalize_from_file(str(bad))

    def test_denormalize_from_folder(
        self, tmp_path, denormalizer,
        sample_patient, sample_observation, sample_appointment):
        for r in (sample_patient, sample_observation, sample_appointment):
            FileHandler.write_resource(
                str(tmp_path / f"{r['resourceType']}_{r['id']}.json"), r
            )
        out = denormalizer.denormalize_from_folder(str(tmp_path))
        assert len(out) == 3
        assert all("_search" in r for r in out)

    def test_denormalize_from_folder_filtered(
        self, tmp_path, denormalizer,
        sample_patient, sample_observation, sample_appointment):
        for r in (sample_patient, sample_observation, sample_appointment):
            FileHandler.write_resource(
                str(tmp_path / f"{r['resourceType']}_{r['id']}.json"), r
            )
        out = denormalizer.denormalize_from_folder(
            str(tmp_path), resource_type="Observation"
        )
        assert len(out) == 1
        assert out[0]["resourceType"] == "Observation"

    def test_denormalize_from_folder_recursive(
        self, tmp_path, denormalizer, sample_patient, sample_observation):
        sub = tmp_path / "sub"
        sub.mkdir()
        FileHandler.write_resource(str(tmp_path / "p.json"), sample_patient)
        FileHandler.write_resource(str(sub / "o.json"), sample_observation)
        flat = denormalizer.denormalize_from_folder(str(tmp_path))
        deep = denormalizer.denormalize_from_folder(str(tmp_path), recursive=True)
        assert len(flat) == 1
        assert len(deep) == 2

    def test_denormalize_from_folder_missing(self, tmp_path, denormalizer):
        with pytest.raises(DenormalizationError):
            denormalizer.denormalize_from_folder(str(tmp_path / "missing"))

    def test_denormalize_from_folder_path_is_file(
        self, tmp_path, denormalizer, sample_patient
    ):
        f = tmp_path / "p.json"
        FileHandler.write_resource(str(f), sample_patient)
        with pytest.raises(DenormalizationError):
            denormalizer.denormalize_from_folder(str(f))

    def test_denormalize_field_known(self, denormalizer, sample_observation):
        out = denormalizer.denormalize_field(
            "code", sample_observation["code"], "Observation"
        )
        assert "code_codes" in out
        assert "8480-6" in out["code_codes"]

    def test_denormalize_field_unknown_field(self, denormalizer, sample_observation):
        out = denormalizer.denormalize_field(
            "totally-unknown", "value", "Observation"
        )
        assert out == {}

    def test_denormalize_field_unknown_resource(self, denormalizer):
        out = denormalizer.denormalize_field("code", "x", "DoesNotExist")
        assert out == {}

    def test_validate_passes_for_minimal(self, denormalizer):
        # Resource without source fields needing denormalization → valid
        assert denormalizer.validate(
            {"resourceType": "Observation", "id": "1", "status": "final"}
        )

    def test_validate_unknown_resource_type_returns_true(self, denormalizer):
        assert denormalizer.validate(
            {"resourceType": "NotConfigured", "id": "x"}
        )

    def test_validate_missing_resourcetype_raises(self, denormalizer):
        with pytest.raises(ValidationError):
            denormalizer.validate({"id": "no-type"})

    def test_validate_after_denormalize(self, denormalizer, sample_observation):
        out = denormalizer.denormalize(sample_observation)
        assert denormalizer.validate(out) is True

    def test_validate_raises_when_search_missing_required(self, denormalizer):
        # A resource that has source fields but no _search should fail validation
        bad = {
            "resourceType": "Observation",
            "id": "missing-search",
            "status": "final",
            "code": {"coding": [{"system": "http://x", "code": "y"}]},
            "subject": {"reference": "Patient/p1"},
        }
        with pytest.raises(ValidationError):
            denormalizer.validate(bad)


# ===========================================================================
# 4) MongoDB workflows — real localhost:27017
# ===========================================================================

def _mongo_available() -> bool:
    try:
        from pymongo import MongoClient
        client = MongoClient(
            "mongodb://localhost:27017", serverSelectionTimeoutMS=1500
        )
        client.admin.command("ping")
        client.close()
        return True
    except Exception:
        return False


@pytest.mark.mongodb
@pytest.mark.skipif(not _mongo_available(), reason="MongoDB not running on localhost:27017")
class TestMongoDBHandler:
    """Cover MongoDBHandler against a real MongoDB at localhost:27017."""

    @pytest.fixture
    def collection(self):
        from pymongo import MongoClient
        client = MongoClient("mongodb://localhost:27017")
        db = client["fhir_search_to_mql_tests"]
        coll = db["mongo_handler_tests"]
        coll.delete_many({})
        yield coll
        coll.delete_many({})
        client.close()

    @pytest.fixture
    def target_collection(self):
        from pymongo import MongoClient
        client = MongoClient("mongodb://localhost:27017")
        db = client["fhir_search_to_mql_tests"]
        coll = db["mongo_handler_tests_target"]
        coll.delete_many({})
        yield coll
        coll.delete_many({})
        client.close()

    @pytest.fixture
    def seed(self, collection, sample_patient, sample_observation, sample_appointment):
        # Write 3 distinct resources
        MongoDBHandler.write_resources(
            collection,
            [sample_patient, sample_observation, sample_appointment])
        return collection

    def test_write_resources_empty_returns_zero(self, collection):
        assert MongoDBHandler.write_resources(collection, []) == 0

    def test_write_and_read_resources(self, seed):
        all_docs = MongoDBHandler.read_resources(seed)
        assert len(all_docs) == 3

    def test_read_with_query_and_projection(self, seed):
        docs = MongoDBHandler.read_resources(
            seed, query={"resourceType": "Observation"},
            projection={"resourceType": 1, "id": 1})
        assert len(docs) == 1
        assert "resourceType" in docs[0]

    def test_read_with_limit(self, seed):
        docs = MongoDBHandler.read_resources(seed, limit=2)
        assert len(docs) == 2

    def test_update_search_fields(self, seed, sample_patient):
        denormalizer = ResourceDenormalizer()
        stats = MongoDBHandler.update_search_fields(
            seed, processor=denormalizer.denormalize
        )
        assert stats["processed"] == 3
        assert stats["updated"] >= 1
        # Verify _search was actually written for at least the Observation
        obs = seed.find_one({"resourceType": "Observation"})
        assert "_search" in obs

    def test_update_search_fields_requires_processor(self, seed):
        with pytest.raises(DenormalizationError):
            MongoDBHandler.update_search_fields(seed)

    def test_batch_process_returns_results(self, seed):
        denormalizer = ResourceDenormalizer()
        out = MongoDBHandler.batch_process(seed, processor=denormalizer.denormalize)
        assert len(out) == 3

    def test_batch_process_update_in_place(self, seed):
        denormalizer = ResourceDenormalizer()
        out = MongoDBHandler.batch_process(
            seed, processor=denormalizer.denormalize, update_in_place=True
        )
        assert out == []
        # _search field should be present
        for doc in seed.find({}):
            # Patient/Observation/Appointment all have denorm rules so all
            # should now have _search.
            assert "_search" in doc

    def test_batch_process_requires_processor(self, seed):
        with pytest.raises(DenormalizationError):
            MongoDBHandler.batch_process(seed)

    def test_collection_stats(self, seed):
        # Add _search to one doc, then check stats
        seed.update_one(
            {"resourceType": "Observation"},
            {"$set": {"_search": {"x": 1}}})
        stats = MongoDBHandler.get_collection_stats(seed)
        assert stats["total_count"] == 3
        assert stats["with_search"] == 1
        assert stats["without_search"] == 2
        assert stats["resource_types"] == {
            "Patient": 1, "Observation": 1, "Appointment": 1
        }

    def test_collection_stats_with_filter(self, seed):
        stats = MongoDBHandler.get_collection_stats(seed, resource_type="Patient")
        assert stats["total_count"] == 1
        assert stats["resource_types"] == {"Patient": 1}

    def test_ensure_indexes_creates_them(self, seed):
        index_specs = [
            {
                "fields": {"resourceType": 1, "id": 1},
                "options": {"name": "idx_rt_id"},
            },
            {
                "fields": [("status", 1)],
                "options": {"name": "idx_status"},
            },
        ]
        names = MongoDBHandler.ensure_indexes(seed, index_specs)
        assert "idx_rt_id" in names
        assert "idx_status" in names
        existing = list(seed.list_indexes())
        existing_names = {ix["name"] for ix in existing}
        assert "idx_rt_id" in existing_names
        assert "idx_status" in existing_names

    def test_remove_search_fields(self, seed):
        denormalizer = ResourceDenormalizer()
        MongoDBHandler.update_search_fields(seed, processor=denormalizer.denormalize)
        modified = MongoDBHandler.remove_search_fields(seed)
        assert modified >= 1
        # After removal, no doc should have a _search field
        assert seed.count_documents({"_search": {"$exists": True}}) == 0

    def test_copy_collection(self, seed, target_collection):
        copied = MongoDBHandler.copy_collection(seed, target_collection)
        assert copied == 3
        assert target_collection.count_documents({}) == 3

    def test_copy_collection_with_processor(self, seed, target_collection):
        def processor(doc):
            doc["_copied"] = True
            return doc

        copied = MongoDBHandler.copy_collection(
            seed, target_collection, processor=processor
        )
        assert copied == 3
        assert target_collection.count_documents({"_copied": True}) == 3

    def test_copy_collection_with_query(self, seed, target_collection):
        copied = MongoDBHandler.copy_collection(
            seed, target_collection, query={"resourceType": "Patient"}
        )
        assert copied == 1


@pytest.mark.mongodb
@pytest.mark.skipif(not _mongo_available(), reason="MongoDB not running on localhost:27017")
class TestResourceDenormalizerMongoDB:
    """ResourceDenormalizer.denormalize_from_mongodb against real MongoDB."""

    @pytest.fixture
    def collection(self):
        from pymongo import MongoClient
        client = MongoClient("mongodb://localhost:27017")
        db = client["fhir_search_to_mql_tests"]
        coll = db["denorm_mongo"]
        coll.delete_many({})
        yield coll
        coll.delete_many({})
        client.close()

    def test_denormalize_from_mongodb_returns_results(
        self, collection, sample_patient, sample_observation
    ):
        collection.insert_many(
            [dict(sample_patient), dict(sample_observation)]
        )
        d = ResourceDenormalizer()
        results = d.denormalize_from_mongodb(collection)
        assert len(results) == 2
        assert any("_search" in r for r in results)

    def test_denormalize_from_mongodb_update_in_place(
        self, collection, sample_observation
    ):
        # Insert a copy so the underlying _id is generated
        collection.insert_one(dict(sample_observation))
        d = ResourceDenormalizer()
        out = d.denormalize_from_mongodb(collection, update_in_place=True)
        assert out == []
        doc = collection.find_one({})
        assert "_search" in doc
        assert "code_codes" in doc["_search"]

    def test_denormalize_from_mongodb_with_query(
        self, collection, sample_patient, sample_observation
    ):
        collection.insert_many(
            [dict(sample_patient), dict(sample_observation)]
        )
        d = ResourceDenormalizer()
        results = d.denormalize_from_mongodb(
            collection, query={"resourceType": "Observation"}
        )
        assert len(results) == 1
        assert results[0]["resourceType"] == "Observation"
