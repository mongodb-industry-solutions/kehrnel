"""Secure, version-tolerant extraction of the Define-XML metadata needed by ingestion."""

from __future__ import annotations

from typing import Dict, List, Optional

from lxml import etree
from pydantic import BaseModel, ConfigDict, Field


class DefineVariable(BaseModel):
    item_oid: str = Field(alias="itemOID")
    name: str
    label: Optional[str] = None
    data_type: str = Field(alias="dataType")
    length: Optional[int] = None
    order_number: int = Field(alias="orderNumber")
    key_sequence: Optional[int] = Field(default=None, alias="keySequence")
    mandatory: Optional[str] = None
    role: Optional[str] = None

    model_config = ConfigDict(populate_by_name=True)


class DefineDataset(BaseModel):
    item_group_oid: str = Field(alias="itemGroupOID")
    name: str
    label: Optional[str] = None
    purpose: Optional[str] = None
    structure: Optional[str] = None
    domain_keys: List[str] = Field(default_factory=list, alias="domainKeys")
    variables: List[DefineVariable] = Field(default_factory=list)

    model_config = ConfigDict(populate_by_name=True)


class DefineDocument(BaseModel):
    file_oid: Optional[str] = Field(default=None, alias="fileOID")
    study_oid: str = Field(alias="studyOID")
    metadata_version_oid: str = Field(alias="metaDataVersionOID")
    define_version: Optional[str] = Field(default=None, alias="defineVersion")
    standard_name: Optional[str] = Field(default=None, alias="standardName")
    standard_version: Optional[str] = Field(default=None, alias="standardVersion")
    datasets: Dict[str, DefineDataset]

    model_config = ConfigDict(populate_by_name=True)


def _local_name(element: etree._Element) -> str:
    # lxml exposes comments and processing instructions during ``iter()``;
    # their ``tag`` is a callable sentinel rather than a QName-compatible name.
    if not isinstance(element.tag, str):
        return ""
    return etree.QName(element).localname


def _attribute(element: etree._Element, local_name: str) -> Optional[str]:
    for key, value in element.attrib.items():
        if etree.QName(key).localname == local_name:
            return value
    return None


def _children(element: etree._Element, local_name: str):
    return [child for child in element if _local_name(child) == local_name]


def parse_define_xml(content: bytes, *, max_bytes: int = 10_000_000) -> DefineDocument:
    if not isinstance(content, bytes):
        raise TypeError("Define-XML content must be bytes")
    if len(content) > max_bytes:
        raise ValueError(f"Define-XML exceeds the configured {max_bytes}-byte limit")
    if b"<!DOCTYPE" in content.upper():
        raise ValueError("Define-XML documents containing a DOCTYPE are not accepted")
    parser = etree.XMLParser(resolve_entities=False, no_network=True, load_dtd=False, recover=False, huge_tree=False)
    root = etree.fromstring(content, parser=parser)
    studies = [element for element in root.iter() if _local_name(element) == "Study"]
    metadata_versions = [element for element in root.iter() if _local_name(element) == "MetaDataVersion"]
    if len(studies) != 1 or len(metadata_versions) != 1:
        raise ValueError("Define-XML must contain exactly one Study and one MetaDataVersion")
    study = studies[0]
    metadata_version = metadata_versions[0]
    study_oid = _attribute(study, "OID")
    metadata_version_oid = _attribute(metadata_version, "OID")
    if not study_oid or not metadata_version_oid:
        raise ValueError("Define-XML Study and MetaDataVersion OIDs are required")

    item_definitions = {
        _attribute(element, "OID"): element
        for element in metadata_version.iter()
        if _local_name(element) == "ItemDef" and _attribute(element, "OID")
    }
    datasets: Dict[str, DefineDataset] = {}
    for item_group in (element for element in metadata_version.iter() if _local_name(element) == "ItemGroupDef"):
        item_group_oid = _attribute(item_group, "OID")
        name = _attribute(item_group, "Name")
        if not item_group_oid or not name:
            continue
        domain_keys = [part.strip() for part in (_attribute(item_group, "DomainKeys") or "").split(",") if part.strip()]
        key_positions = {variable_name: index for index, variable_name in enumerate(domain_keys, start=1)}
        variables: List[DefineVariable] = []
        for fallback_order, item_ref in enumerate(_children(item_group, "ItemRef"), start=1):
            item_oid = _attribute(item_ref, "ItemOID")
            item_def = item_definitions.get(item_oid)
            if not item_oid or item_def is None:
                continue
            variable_name = _attribute(item_def, "Name")
            data_type = _attribute(item_def, "DataType")
            if not variable_name or not data_type:
                continue
            length_value = _attribute(item_def, "Length")
            order_value = _attribute(item_ref, "OrderNumber")
            variables.append(
                DefineVariable(
                    itemOID=item_oid,
                    name=variable_name,
                    label=_attribute(item_def, "Label"),
                    dataType="string" if data_type == "text" else data_type,
                    length=int(length_value) if length_value and length_value.isdigit() else None,
                    orderNumber=int(order_value) if order_value and order_value.isdigit() else fallback_order,
                    keySequence=key_positions.get(variable_name),
                    mandatory=_attribute(item_ref, "Mandatory"),
                    role=_attribute(item_ref, "Role"),
                )
            )
        dataset = DefineDataset(
            itemGroupOID=item_group_oid,
            name=name,
            label=_attribute(item_group, "Label"),
            purpose=_attribute(item_group, "Purpose"),
            structure=_attribute(item_group, "Structure"),
            domainKeys=domain_keys,
            variables=sorted(variables, key=lambda variable: variable.order_number),
        )
        datasets[name.upper()] = dataset
    if not datasets:
        raise ValueError("Define-XML does not contain any usable ItemGroupDef datasets")
    return DefineDocument(
        fileOID=_attribute(root, "FileOID"),
        studyOID=study_oid,
        metaDataVersionOID=metadata_version_oid,
        defineVersion=_attribute(metadata_version, "DefineVersion"),
        standardName=_attribute(metadata_version, "StandardName"),
        standardVersion=_attribute(metadata_version, "StandardVersion"),
        datasets=datasets,
    )
