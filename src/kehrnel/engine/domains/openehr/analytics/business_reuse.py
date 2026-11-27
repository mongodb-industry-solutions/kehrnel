"""Explicit reuse of a business variable across distinct template bindings.

The existing variable owns the output contract. A source mapping keeps its own
identity and an immutable reuse decision; matching labels never imply equivalence.
"""
from copy import deepcopy
from .source import digest, segments, selector_parts
from .model_config import require

CONTRACT = 'openehr-business-reuse/v1'


def source_definition(path):
    """Structural candidate identity, not proof of clinical interchangeability."""
    parts = segments(path)
    roots = [(i, selector_parts(selector)[0]) for i, (_, selector) in enumerate(parts)
             if selector and selector_parts(selector)[0].startswith('openEHR-EHR-')]
    if not roots:
        return None
    index, archetype = roots[-1]
    relative = ''.join('/' + attribute + ('[' + selector + ']' if selector else '')
                       for attribute, selector in parts[index + 1:])
    return {'archetype_id': archetype, 'relative_path': relative}


def variable_contract(variable):
    return {k: deepcopy(variable.get(k)) for k in ('key', 'semantic_key', 'type', 'unit')} | {
        'rm_types': sorted(variable.get('rm_types', []))
    }


def all_fields(template):
    return [b for g in template['groups'] for b in g['fields']] + template.get('activity_fields', [])


def validate_reuse(template, variables):
    """Validate persisted configuration too, without a catalog/database lookup."""
    for binding in all_fields(template):
        decision = binding.get('business_reuse')
        if decision is None:
            continue
        require(isinstance(decision, dict) and set(decision) == {
            'contract', 'source_semantic_key', 'source_definition', 'source_rm_types',
            'target_rule_revision', 'target_contract', 'target_contract_digest', 'reason',
            'shape_policy', 'source_multiple'
        }, 'Invalid business-variable reuse decision')
        require(decision['contract'] == CONTRACT, 'Unsupported business-variable reuse contract')
        require(isinstance(decision['reason'], str) and 1 <= len(decision['reason'].strip()) <= 2000,
                'Explain why this source may feed the existing business variable')
        require(isinstance(decision['source_semantic_key'], str)
                and decision['source_semantic_key'].startswith('openehr-semantic-key:sha256:'),
                'Reuse must retain the original template semantic identity')
        require(isinstance(decision['target_rule_revision'], str)
                and len(decision['target_rule_revision']) == 64,
                'Pin the target variable rule revision')
        require(decision['shape_policy'] == 'preserve_source'
                and type(decision['source_multiple']) is bool
                and decision['source_multiple'] == bool(binding.get('multiple')),
                'Reuse must preserve its declared source value shape')
        target = variable_contract(variables[binding['key']])
        require(decision['target_contract'] == target
                and decision['target_contract_digest'] == digest(target),
                'The reused business-variable contract changed; review the binding again')
        require(decision['source_definition'] is not None
                and decision['source_definition'] == source_definition(binding['path']),
                'Reuse source definition differs from its bound archetype path')
        types = decision['source_rm_types']
        require(isinstance(types, list) and bool(types)
                and set(types) <= set(target['rm_types']),
                'Source value types exceed the reused business-variable contract')


def apply_reuse(value, selections, prior_rules):
    """Authoring only: compile explicit source-field → existing-variable decisions.

    This first contract supports the same versioned archetype element, compatible
    output representation/unit, and source RM variants covered by the target.
    Context still requires human review. Different archetypes are not merged.
    """
    require(isinstance(selections, dict), 'Reuse variables by selected source field ID')
    if not selections:
        return []
    from .rules import RuleRevision
    retained = {r.revision: r for r in (RuleRevision.compile(x) for x in prior_rules)}
    template = value['catalog']['templates'][0]
    variables = {v['key']: v for v in value['catalog']['variables']}
    fields = all_fields(template)
    before = deepcopy(variables)
    selected = {b.get('field_id') for b in fields}
    require(set(selections) <= selected, 'Reuse only selected, resolved source fields')
    updates = []
    for field_id, selection in selections.items():
        require(isinstance(selection, dict) and set(selection) == {'key', 'rule_revision', 'reason', 'shape_policy'},
                'Reuse needs an existing key, exact rule revision, reason and shape policy')
        require(isinstance(selection['key'], str) and isinstance(selection['rule_revision'], str),
                'Reuse target key and rule revision must be strings')
        target_rule = retained.get(selection['rule_revision'])
        require(target_rule is not None, 'Reuse target revision is not in the retained configuration')
        target = target_rule.catalog.variables.get(selection['key'])
        require(target is not None and target.get('semantic_key'), 'Unknown reusable business variable')
        source_bindings = [b for b in fields if b.get('field_id') == field_id]
        source_keys = {b['key'] for b in source_bindings}
        require(len(source_keys) == 1, 'A source field must have one current output contract')
        old_key = next(iter(source_keys))
        original = before[old_key]
        require(original.get('semantic_key'), 'Source field has no template semantic identity')
        target_bindings = [b for b in all_fields(target_rule.catalog.templates[target_rule.template])
                           if b['key'] == target['key']]
        definition = source_definition(source_bindings[0]['path'])
        require(definition is not None and target_bindings
                and all(source_definition(b['path']) == definition for b in target_bindings + source_bindings),
                'Reuse requires the same versioned archetype element; broader equivalence is not supported')
        require((original['type'], original.get('unit')) == (target['type'], target.get('unit')),
                'Reuse requires the same output type and canonical unit; configure conversion explicitly')
        require(bool(original.get('rm_types')) and set(original['rm_types']) <= set(target.get('rm_types', [])),
                'Source value types exceed the reused business-variable contract')
        require(selection['shape_policy'] == 'preserve_source'
                and all(bool(b.get('multiple')) == bool(source_bindings[0].get('multiple'))
                        for b in source_bindings),
                'Reuse preserves source arrays; it cannot collapse repeated values')
        signature = variable_contract(target)
        decision = {
            'contract': CONTRACT, 'source_semantic_key': original['semantic_key'],
            'source_definition': definition, 'source_rm_types': sorted(original['rm_types']),
            'target_rule_revision': target_rule.revision, 'target_contract': signature,
            'target_contract_digest': digest(signature), 'reason': selection['reason'],
            'shape_policy': 'preserve_source', 'source_multiple': bool(source_bindings[0].get('multiple')),
        }
        for binding in source_bindings:
            binding['key'] = target['key']
            binding['business_reuse'] = deepcopy(decision)
        for group in template['groups']:
            if any(b in source_bindings for b in group['fields']):
                d = group['definition']
                d['required_fields'] = [target['key'] if k == old_key else k for k in d['required_fields']]
                if d.get('time_key') == old_key: d['time_key'] = target['key']
                if group.get('time_key') == old_key: group['time_key'] = target['key']
        if target['key'] in variables and target['key'] != old_key:
            # A conflicting field in this fact must fail, never overwrite a value.
            require(variable_contract(variables[target['key']]) == signature,
                    'Target key already has a different business contract')
        variables[target['key']] = deepcopy(target)
        updates.append({'field_id': field_id, 'from_key': old_key, 'to_key': target['key'], **decision})
    used = {b['key'] for b in fields}
    value['catalog']['variables'] = [v for k, v in variables.items() if k in used]
    validate_reuse(template, variables)
    return updates
