from pathlib import Path

import yaml


def test_public_ec2_release_matches_production_branch():
    root = Path(__file__).resolve().parents[2]
    workflow = yaml.load((root / ".github/workflows/deploy.yml").read_text(), Loader=yaml.BaseLoader)
    assert workflow["on"]["push"]["branches"] == ["main"]
    build = next(step for step in workflow["jobs"]["deploy"]["steps"] if "ECR_REPOSITORY" in step.get("env", {}))
    assert build["env"]["ECR_REPOSITORY"] == "industrysolutions/kehrnel"
