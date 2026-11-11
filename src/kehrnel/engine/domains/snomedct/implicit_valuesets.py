"""SNOMED implicit FHIR ValueSets are ECL requests, not tenant registry entries."""
from __future__ import annotations
import re
from urllib.parse import urlsplit, parse_qsl
from kehrnel.engine.core.errors import KehrnelError

SYSTEM = 'http://snomed.info/sct'
_PATH = re.compile(r'^/sct(?:/(?P<edition>\d+))?(?:/version/(?P<release>\d{8}))?/?$')

def implicit_snomed_valueset(url: str) -> dict[str, str] | None:
    parsed = urlsplit(str(url or '').strip())
    if parsed.scheme not in {'http', 'https'} or parsed.hostname != 'snomed.info':
        return None
    selectors = [(k,v) for k,v in parse_qsl(parsed.query,keep_blank_values=True) if k=='fhir_vs']
    if not selectors:
        return None
    match = _PATH.fullmatch(parsed.path)
    if not match or parsed.username or parsed.password or parsed.port or parsed.fragment or len(selectors)!=1:
        raise KehrnelError(code='SNOMED_IMPLICIT_VALUE_SET_INVALID',status=422,message='Provide one valid SNOMED implicit ValueSet selector.')
    selector = selectors[0][1].strip()
    if selector.lower().startswith('ecl/') and selector[4:].strip():
        expression=selector[4:].strip()
    elif selector.lower().startswith('refset/') and re.fullmatch(r'\d{6,18}',selector[7:].strip()):
        expression='^ '+selector[7:].strip()
    else:
        raise KehrnelError(code='SNOMED_IMPLICIT_VALUE_SET_UNSUPPORTED',status=422,message='Use an ecl/ expression or refset/ identifier for an implicit SNOMED ValueSet.')
    result={'system':SYSTEM,'expression':expression,'url':str(url).strip()}
    if match.group('release'):
        edition=match.group('edition')
        result['version']=SYSTEM+(('/'+edition) if edition else '')+'/version/'+match.group('release')
    return result
