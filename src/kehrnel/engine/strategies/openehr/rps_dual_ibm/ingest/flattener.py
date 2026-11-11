"""IBM-exact flattener for openEHR RPS-dual documents."""

from __future__ import annotations

from copy import deepcopy
import json
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
import uuid

from bson.binary import Binary, UuidRepresentation

from kehrnel.engine.strategies.openehr.rps_dual.ingest.flattener import (
    NON_ARCHETYPED_RM,
    SKIP_ATTRS,
    CompositionFlattener,
)


class IBMCompositionFlattener(CompositionFlattener):
    """Produce documents compatible with the IBM compact openEHR model."""

    _UUID_ENVELOPE_TARGETS = {"_id", "template_id"}

    @classmethod
    async def create(
        cls,
        db,
        config: dict,
        mappings_path: str,
        mappings_content=None,
        field_map: Optional[Dict[str, Dict[str, str]]] = None,
        coding_opts: Optional[Dict[str, Any]] = None,
    ):
        instance = cls(
            db,
            config,
            mappings_path,
            mappings_content,
            field_map=field_map,
            coding_opts=coding_opts,
        )
        if instance.use_codes_db:
            if instance.db is not None:
                await instance._load_codes_from_db()
            else:
                instance._load_codes_from_bundle()
        if instance.apply_shortcuts:
            if instance.db is not None:
                await instance._load_shortcuts_from_db()
            else:
                instance._load_shortcuts_from_bundle()
        if instance.catalog_mappings_spec:
            await instance._load_mappings_from_catalog(instance.catalog_mappings_spec)
        instance._refresh_codec()
        return instance

    def _bundle_path(self, *parts: str) -> Path:
        return Path(__file__).resolve().parents[1] / "bundles" / Path(*parts)

    def _load_codes_from_bundle(self) -> None:
        path = self._bundle_path("dictionaries", "_codes.json")
        doc = json.loads(path.read_text(encoding="utf-8"))

        ar_book: Dict[str, Any] = {}
        for rm, subtree in doc.items():
            if rm in ("_id", "_max", "_min", "unknown", "at"):
                continue
            if not isinstance(subtree, dict):
                continue
            for name, vers in subtree.items():
                if not isinstance(vers, dict):
                    continue
                for ver, code in vers.items():
                    ar_book[f"{rm}.{name}.{ver}"] = code

        self.code_book["ar_code"] = ar_book
        self.code_book["at"] = doc.get("at") or {}
        self.seq["ar_code"] = int(doc.get("_max") or self.seq.get("ar_code") or 0)
        self._refresh_codec()

    def _load_shortcuts_from_bundle(self) -> None:
        path = self._bundle_path("shortcuts", "shortcuts.json")
        doc = json.loads(path.read_text(encoding="utf-8"))
        self.shortcut_keys.update(doc.get("items") or {})
        self.shortcut_keys.update(doc.get("keys") or {})
        self.shortcut_vals.update(doc.get("values") or {})
        self._refresh_codec()

    def _encode_id(self, value: Any, field: str) -> Any:
        policy = (self.id_encoding.get(field) or "string").lower()
        if policy in ("uuid", "uuidbin", "uuid_bin"):
            try:
                return Binary.from_uuid(
                    uuid.UUID(str(value)),
                    uuid_representation=UuidRepresentation.STANDARD,
                )
            except Exception:
                return value
        return super()._encode_id(value, field)

    def _alloc_code(self, key: str, sid: str) -> Any:
        code = super()._alloc_code(key, sid)
        if key == "ar_code" and code is not None and not isinstance(code, str):
            code = str(code)
            self.code_book.setdefault(key, {})[sid] = code
        return code

    def _apply_sc_deep(self, o):
        if isinstance(o, dict):
            return {self.shortcut_keys.get(k, k): self._apply_sc_deep(v) for k, v in o.items()}
        if isinstance(o, list):
            return [self._apply_sc_deep(x) for x in o]
        if isinstance(o, str) and o in self.shortcut_vals:
            return f"$>{self.shortcut_vals[o]}"
        return o

    def _prepare_ibm_archetype_details(self, scalars: Dict[str, Any]) -> None:
        ad = scalars.get("archetype_details") or scalars.get("ad")
        if not isinstance(ad, dict):
            return

        ai_obj = ad.get("archetype_id") or ad.get("ai")
        if isinstance(ai_obj, dict):
            sid = ai_obj.get("value") or ai_obj.get("v")
            if isinstance(sid, str):
                ai_obj["value"] = str(self._alloc_code("ar_code", sid))
        elif isinstance(ai_obj, str):
            ad["archetype_id"] = {"value": str(self._alloc_code("ar_code", ai_obj))}

    def _normalize_ibm_envelope_value(self, target_path: str, value: Any) -> Any:
        if target_path in self._UUID_ENVELOPE_TARGETS:
            try:
                return Binary.from_uuid(
                    uuid.UUID(str(value)),
                    uuid_representation=UuidRepresentation.STANDARD,
                )
            except Exception:
                return value
        if target_path == "creation_date":
            return self._normalize_top_level_datetime(value)
        return value

    def _apply_envelope_fields(
        self,
        target: Dict[str, Any],
        source_envelope: Dict[str, Any],
        mapping: Dict[str, str],
    ) -> None:
        if not isinstance(target, dict) or not isinstance(source_envelope, dict) or not isinstance(mapping, dict):
            return

        for source_path, target_path in mapping.items():
            found, value = self._lookup_envelope_value(source_envelope, source_path)
            if not found:
                continue
            self._assign_envelope_value(
                target,
                target_path,
                deepcopy(self._normalize_ibm_envelope_value(target_path, value)),
            )

    def _walk(
        self,
        node: dict,
        anc_codes: Tuple[int, ...],
        anc_pi: Tuple[int, ...],
        cn: List[dict],
        *,
        kp_chain: List[str],
        list_index: Optional[int],
    ):
        aid = self._archetype_id(node)
        if aid:
            code = self._at_code_to_int(aid) if aid.lower().startswith("at") else self._alloc_code("ar_code", aid)
        else:
            code = "0"

        is_root = not anc_codes
        emit = (list_index is not None or is_root or self._split_me_as_a_new_node(node)) and code is not None
        cur_li = list_index if list_index is not None else None

        scalars: Dict[str, Any] = {}
        for k, v in node.items():
            if k in SKIP_ATTRS:
                continue

            if isinstance(v, dict) and self._is_locatable(v) and self._split_me_as_a_new_node(v):
                continue

            if isinstance(v, dict) and self._is_locatable(v) and v.get("_type") in NON_ARCHETYPED_RM:
                scalars[k] = self._strip_locatables(v)
                continue

            if isinstance(v, list) and any(self._is_locatable(x) and self._split_me_as_a_new_node(x) for x in v):
                continue

            scalars[k] = v

        scalars = self._to_bson_dates(scalars)
        scalars["archetype_node_id"] = str(code)
        self._prepare_ibm_archetype_details(scalars)

        payload = scalars

        if emit:
            leaf_path = self.path_separator.join([str(code)] + [str(a) for a in reversed(anc_codes)])
            cn_node: Dict[str, Any] = {
                self.cf_data: payload,
                self.cf_path: leaf_path,
                "_anc": anc_codes,
            }

            if self.cf_ap and aid:
                cn_node[self.cf_ap] = aid
            if kp_chain:
                cn_node["kp"] = kp_chain[:]
            if cur_li is not None:
                cn_node[self.cf_pi] = int(cur_li)

            cn.append(cn_node)

        new_anc_codes = anc_codes + ((code,) if emit else ())
        new_anc_pi = anc_pi + (((cur_li if cur_li is not None else -1),) if emit else ())
        base_kp = [] if emit else kp_chain

        for k, v in node.items():
            k_long = self._sc_key(k)

            if isinstance(v, dict) and self._is_locatable(v):
                self._walk(v, new_anc_codes, new_anc_pi, cn, kp_chain=base_kp + [k_long], list_index=None)
            elif isinstance(v, list):
                for idx, itm in enumerate(v):
                    if self._is_locatable(itm):
                        self._walk(itm, new_anc_codes, new_anc_pi, cn, kp_chain=base_kp + [k_long], list_index=idx)
