import pytest

from kehrnel.engine.domains.cdisc.define_xml import parse_define_xml


DEFINE = b'''<?xml version="1.0"?>
<ODM xmlns="http://www.cdisc.org/ns/odm/v1.3" xmlns:def="http://www.cdisc.org/ns/def/v2.1" FileOID="FILE.1">
  <Study OID="STUDY.1"><MetaDataVersion OID="MDV.1" def:DefineVersion="2.1" def:StandardName="SDTM" def:StandardVersion="3.4">
    <ItemGroupDef OID="IG.DM" Name="DM" Purpose="Tabulation" def:Label="Demographics" def:DomainKeys="STUDYID, USUBJID">
      <ItemRef ItemOID="IT.STUDYID" OrderNumber="1" Mandatory="Yes"/>
      <ItemRef ItemOID="IT.USUBJID" OrderNumber="2" Mandatory="Yes"/>
    </ItemGroupDef>
    <ItemDef OID="IT.STUDYID" Name="STUDYID" DataType="text" Length="20" def:Label="Study Identifier"/>
    <ItemDef OID="IT.USUBJID" Name="USUBJID" DataType="text" Length="40" def:Label="Subject Identifier"/>
  </MetaDataVersion></Study>
</ODM>'''


def test_define_xml_extracts_version_independent_metadata():
    document = parse_define_xml(DEFINE)

    assert document.study_oid == "STUDY.1"
    assert document.standard_version == "3.4"
    assert document.datasets["DM"].domain_keys == ["STUDYID", "USUBJID"]
    assert [variable.key_sequence for variable in document.datasets["DM"].variables] == [1, 2]


def test_define_xml_ignores_comments_and_processing_instructions():
    decorated = DEFINE.replace(
        b'<ODM ',
        b'<?xml-stylesheet type="text/xsl" href="define.xsl"?>\n<!-- vendor metadata -->\n<ODM ',
    ).replace(b'<Study ', b'<!-- study section -->\n  <Study ')

    document = parse_define_xml(decorated)

    assert document.study_oid == "STUDY.1"
    assert "DM" in document.datasets


def test_define_xml_rejects_doctype_and_oversize_input():
    with pytest.raises(ValueError, match="DOCTYPE"):
        parse_define_xml(b'<!DOCTYPE foo [<!ENTITY xxe SYSTEM "file:///etc/passwd">]><foo/>')
    with pytest.raises(ValueError, match="exceeds"):
        parse_define_xml(DEFINE, max_bytes=10)
