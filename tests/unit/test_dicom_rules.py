import copy

import pytest

from kehrnel.engine.core.errors import KehrnelError
from kehrnel.engine.core.types import StrategyContext
from kehrnel.engine.strategies.dicom.tiered_store import playground, rules


@pytest.fixture
def ctx():
    return StrategyContext(environment_id="rule-test",config={})


def test_profiles_are_bounded_and_do_not_claim_full_modality_support(ctx):
    catalog=playground.catalog(ctx,{})
    assert len(catalog["profiles"])==11
    for profile in catalog["profiles"]:
        assert rules.validate(profile).startswith("v1:")
    assert "Frame/sequence item correlated query compilation" in catalog["unsupported"]
    assert catalog['referenceArchitecture']['artifactRef']['id']=='architecture.healthcare.dicom-metadata'
    assert catalog['referenceArchitecture']['supportStatus']=='evaluation_only'
    assert next(n for n in catalog['runtimeWorkflow'] if n['id']=='archive')['status']=='plan_only'


def test_raw_source_is_preserved_while_rules_create_embedded_and_sidecar_data(ctx):
    source=playground.demo_source(); before=copy.deepcopy(source)
    profile=playground.PROFILES["profiles"][0]
    preview=rules.preview(ctx,{**source,"profile":profile})
    assert source==before
    assert preview["counts"]["uniqueSops"]==12
    summary=preview["collections"]["studies"][0]["se"][0]["_kehrnel"]["ruleSummary"]
    assert summary["groups"][0]["semantics"]=="same_instance_scalar_tuple"
    assert summary["counts"]["receivedUniqueSops"]==6
    assert preview["collections"]["research"][0]["fields"]["manufacturer"]["coverage"]["missingOrEmpty"]==1
    assert not preview["persisted"] and not preview["publishedCorpus"]


def test_same_image_query_rejects_false_combination_but_series_query_exposes_its_grain(ctx):
    query={"text":"Find CT series where rows = 1024 and photometric = MONOCHROME2"}
    broad=playground.demo(ctx,{"query":query})["queryResult"]
    strict=playground.demo(ctx,{"query":{**query,"group":"rendering"}})["queryResult"]
    assert len(broad["rows"])==1
    assert strict["rows"]==[]
    assert strict["semantics"]=="same_instance_scalar_tuple"
    assert strict["pipeline"][0]["$match"]["$and"][1]["groups"]["$elemMatch"]["p"]=="rendering"


def test_two_range_bounds_match_one_attribute_value_in_both_engines(ctx):
    request={"text":"Find series where rows > 600 and rows < 800"}
    result=playground.demo(ctx,{"query":request})["queryResult"]
    assert result["rows"]==[]  # Observed 512 and 1024 cannot manufacture a middle value.
    match=result["pipeline"][0]["$match"]["$and"][1]["attrs"]["$elemMatch"]
    assert match=={"p":"rows","n":{"$gt":600.0,"$lt":800.0}}
    filters=result["searchPipeline"][0]["$search"]["compound"]["filter"]
    element=next(x["embeddedDocument"] for x in filters if "embeddedDocument" in x)
    assert len(element["operator"]["compound"]["filter"])==3


def test_embedded_field_can_also_be_queried_through_its_research_group(ctx):
    profile=copy.deepcopy(playground.PROFILES['profiles'][0])
    next(field for field in profile['fields'] if field['name']=='rows')['target']='embedded'
    result=playground.demo(ctx,{'profile':profile,'query':{
        'text':'Find CT series where rows = 512 and photometric = MONOCHROME2', 'group':'rendering'}})
    assert len(result['queryResult']['rows'])==1
    assert not any(a['p']=='rows' for row in result['preview']['collections']['research'] for a in row['attrs'])


def test_invalid_statistical_rule_fails_before_reception(ctx):
    profile=copy.deepcopy(playground.PROFILES['profiles'][0])
    profile['fields'][0]['aggregate']='range'
    with pytest.raises(KehrnelError) as error:
        rules.validate(profile)
    assert error.value.code=='DICOM_RULE_INVALID'


def test_common_is_not_inferred_from_partial_or_missing_headers(ctx):
    profile=copy.deepcopy(playground.PROFILES["profiles"][0])
    profile["fields"][0]["aggregate"]="common"
    source=playground.demo_source(); source["instances"]=source["instances"][:1]
    result=rules.preview(ctx,{**source,"profile":profile})
    field=result["collections"]["research"][0]["fields"]["manufacturer"]
    assert field["values"]==[]
    assert field["state"]=="partial_or_unknown"


def test_ambiguous_multivalued_group_never_generates_a_cartesian_product(ctx):
    source=playground.demo_source()
    source["instances"][0]["header"]["00280010"]["Value"]=[512,1024]
    result=rules.preview(ctx,{**source,"profile":playground.PROFILES["profiles"][0]})
    assert result["collections"]["research"][0]["groupCoverage"]["rendering"]["ambiguousSops"]==1
    assert sum(g["sopCount"] for g in result["collections"]["research"][0]["groups"] if g["p"]=="rendering")==5


def test_exact_receptions_deduplicate_and_conflicting_sop_bytes_fail(ctx):
    source=playground.demo_source(); source["instances"].append(copy.deepcopy(source["instances"][0]))
    result=rules.preview(ctx,{**source,"profile":playground.PROFILES["profiles"][0]})
    assert result["counts"]["replayedReceptions"]==1
    assert result["counts"]["uniqueSops"]==12
    source["instances"][-1]["sz"]=1
    with pytest.raises(KehrnelError) as error:
        rules.preview(ctx,{**source,"profile":playground.PROFILES["profiles"][0]})
    assert error.value.code=="DICOM_RECEPTION_CONFLICT"


def test_nonnumeric_vr_is_not_reinterpreted_as_a_numeric_measurement(ctx):
    field={"source":"header","path":["00080070"],"type":"number"}
    assert rules.extract({"header":{"00080070":{"vr":"LO","Value":["123"]}}},field)==([],1)


def test_unindexed_or_statistical_fields_cannot_be_used_as_exact_predicates(ctx):
    profile=copy.deepcopy(playground.PROFILES["profiles"][0])
    profile["fields"][2]["aggregate"]="range"
    with pytest.raises(KehrnelError):
        rules.compile_query(ctx,{"profile":profile,"generationId":"a"*64,"where":[{"field":"rows","op":"=","value":600}]})
    with pytest.raises(KehrnelError) as error:
        playground.demo(ctx,{"query":{"text":"Find series where diagnosis = cancer"}})
    assert error.value.code=="DICOM_RULE_FIELD_UNKNOWN"


def test_authorization_cannot_be_expanded_by_a_source_batch(ctx):
    ctx.meta={"authorized_centers":["DEMO_A"]}
    with pytest.raises(KehrnelError) as error:
        playground.demo(ctx,{})
    assert error.value.status==403


def test_changing_modality_filter_reuses_the_same_search_mapping(ctx):
    profile=copy.deepcopy(playground.PROFILES["profiles"][0])
    first=rules.index_plan(rules.config(ctx),profile)
    profile["modalities"]=["CT"]
    assert rules.validate(profile)!=rules.validate(playground.PROFILES["profiles"][0])
    assert rules.index_plan(rules.config(ctx),profile)["search"]["name"]==first["search"]["name"]


def test_collection_collisions_fail_before_writing(ctx):
    cfg=rules.config(ctx)
    cfg["collections"]["studies"]=cfg["collections"]["research"]+"_series"
    with pytest.raises(KehrnelError):
        rules.collection_names(cfg)
