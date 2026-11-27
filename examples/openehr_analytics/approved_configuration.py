"""Download an approved HDL rule bundle once per worker deployment, never per record.

No activation or ingestion. Uses TLS and the environment-scoped read key. Kehrnel
verifies each immutable rule; retain the exact bundle for HDL's delta API.
"""
import argparse
from copy import deepcopy
from hashlib import sha256
import json
import os
from pathlib import Path
from urllib.parse import urlparse
from urllib.request import HTTPRedirectHandler, Request, build_opener

from kehrnel.engine.domains.openehr.analytics import RuleSet

MAX_BYTES = 64 * 1024 * 1024


class NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        raise ValueError('Configuration endpoint redirected; verify the configured URL')


def install_rules(bundle, deployment, *, environment_id):
    """Return a candidate config. This does not change the active worker config."""
    if (bundle.get('contract') != 'hdl.analytics.release-bundle/v1'
            or bundle.get('approval') != 'approved_releases_only'
            or bundle.get('environment', {}).get('id') != environment_id
            or bundle.get('strategy', {}).get('id') != 'openehr.analytics'):
        raise ValueError('Bundle contract, approval scope or environment does not match')
    rules, releases = bundle.get('rules'), bundle.get('releases')
    if not isinstance(rules, list) or not rules or not isinstance(releases, list) or len(rules) != len(releases):
        raise ValueError('A complete, nonempty approved rule bundle is required')
    compiled = RuleSet.compile(rules)
    if len(compiled.by_revision) != len(rules):
        raise ValueError('Bundle contains duplicate rule revisions')
    for rule, release in zip(rules, releases):
        revision = compiled.by_revision.get(release.get('ruleRevision'))
        if (not revision or rule.get('revision') != revision.revision
                or revision.template != release.get('templateId')
                or revision.epoch != release.get('epoch')):
            raise ValueError('Release and immutable rule identity differ')
    candidate = deepcopy(bundle.get("deployment") or deployment)
    # Old approved epochs are essential for replay; the complete bundle replaces rules.
    candidate['rules'] = deepcopy(rules)
    return candidate


def download(base_url, api_key):
    url = urlparse(base_url)
    if url.scheme != 'https' or not url.netloc or url.username or url.password or url.query or url.fragment:
        raise ValueError('Use the HTTPS integration API base URL without credentials or query parameters')
    endpoint = base_url.rstrip('/') + '/strategies/openehr.analytics/configuration?view=bundle'
    req = Request(endpoint, headers={'Authorization': 'Bearer ' + api_key, 'Accept': 'application/json'})
    with build_opener(NoRedirect).open(req, timeout=30) as response:
        raw = response.read(MAX_BYTES + 1)
    if len(raw) > MAX_BYTES:
        raise ValueError('Configuration exceeds this client limit; use a bounded release delivery design')
    return raw, json.loads(raw)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for arg in ('base-url', 'environment-id', 'output', 'bundle'):
        parser.add_argument('--' + arg, required=True)
    parser.add_argument("--deployment", help="Fallback settings for older bundles without deployment configuration")
    args = parser.parse_args()
    outputs = [Path(args.output), Path(args.bundle)]
    if outputs[0].resolve() == outputs[1].resolve() or any(p.exists() for p in outputs):
        raise ValueError('Use two distinct new output filenames; retain the applied baseline')
    raw, bundle = download(args.base_url, os.environ['HDL_INTEGRATION_KEY'])
    candidate = install_rules(bundle, json.loads(Path(args.deployment).read_text()) if args.deployment else {}, environment_id=args.environment_id)
    # Deployment JSON is strategy configuration; credentials belong in the separate env/secret store.
    with outputs[1].open('xb') as handle:
        handle.write(raw)
    with outputs[0].open('x') as handle:
        json.dump(candidate, handle, indent=2, ensure_ascii=False); handle.write('\n')
    print(json.dumps({'downloaded_sequence': bundle['sequence'], 'bundle_digest': bundle['digest'],
                      'response_sha256': sha256(raw).hexdigest(), 'rules': len(candidate['rules']),
                      'installed': False, 'historical_processing_started': False}))


if __name__ == '__main__':
    main()
