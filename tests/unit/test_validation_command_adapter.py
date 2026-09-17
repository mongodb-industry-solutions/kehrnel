import json
import sys

import pytest

from kehrnel.persistence.validation import CommandValidationEngine


@pytest.mark.asyncio
async def test_command_validator_uses_json_envelope_without_a_shell():
    code = (
        "import json,sys; value=json.load(open(sys.argv[1])); "
        "json.dump({'engine':'fake','version':'1','findings':[],"
        "'coverage':{'datasets':len(value['datasets'])}},open(sys.argv[2],'w'))"
    )
    adapter = CommandValidationEngine([sys.executable, "-c", code, "{input}", "{output}"])

    result = await adapter.validate(snapshot={"snapshotId": "v1"}, datasets=[{"dataset": {}}], options={})

    assert result["engine"] == "fake"
    assert result["coverage"] == {"datasets": 1}
