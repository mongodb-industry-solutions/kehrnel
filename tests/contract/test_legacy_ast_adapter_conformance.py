import pytest

from kehrnel.domains.openehr.aql.parse import parse_aql
from kehrnel.domains.openehr.aql.ir import AqlQueryIR
from kehrnel.strategies.openehr.rps_dual.legacy_aql.ast_adapter import adapt_ir_to_legacy_ast
from kehrnel.strategies.openehr.rps_dual.legacy_aql.transformers.ast_validator import ASTValidator


@pytest.mark.parametrize("aql_text", [
    "select Centro as Centro from compositions where ehr_id = 'p1'",
    "select Centro as Centro from compositions where text = 'hello'",
])
def test_adapter_passes_ast_validator(aql_text):
    ir = parse_aql(aql_text)
    legacy_ast = adapt_ir_to_legacy_ast(ir, ehr_alias="e", composition_alias="c")
    ASTValidator.validate_ast(legacy_ast)
