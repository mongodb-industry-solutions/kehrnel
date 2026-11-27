"""OPT-bound structural coverage, independent of CDR conformance validation.

The path index is compiled once per immutable rule. A per-composition collector
shares the metadata validator's traversal and stores no clinical values.
"""
from copy import deepcopy
from dataclasses import dataclass, field
import re
from .source import segments, selector_parts
from .model_config import require

CONTRACT = 'openehr-source-structure/v1'
MAX_ISSUES = 50
STRUCTURAL_TYPES = {
    'COMPOSITION', 'SECTION', 'OBSERVATION', 'EVALUATION', 'ACTION', 'INSTRUCTION',
    'ADMIN_ENTRY', 'CLUSTER', 'ELEMENT', 'HISTORY', 'ITEM_TREE', 'ITEM_LIST',
    'ITEM_SINGLE', 'ITEM_TABLE', 'EVENT', 'POINT_EVENT', 'INTERVAL_EVENT',
    'ACTIVITY', 'EVENT_CONTEXT',
}


def inventory_from_compiler(compiler, semantic):
    return {
        'contract': CONTRACT, 'opt_digest': semantic.template.source_digest,
        'template': semantic.template.template_id,
        'composition_archetype_id': semantic.template.root_archetype_id,
        'nodes': [{'path': p, 'rm_type': f['rm_type']}
                  for p, f in sorted(compiler.frames.items())
                  if not f.get('slot') and f['rm_type'] in STRUCTURAL_TYPES],
        'slots': deepcopy(compiler.slots),
    }


@dataclass
class PathNode:
    # attribute -> {(archetype/node id, fixed name): child}
    children: dict = field(default_factory=dict)
    slots: dict = field(default_factory=dict)
    rm_type: str | None = None
    selected: bool = False


class CoveragePlan:
    @classmethod
    def compile(cls, inventory, template):
        if inventory is None:
            return None
        require(isinstance(inventory, dict) and set(inventory) == {
            'contract','opt_digest','template','composition_archetype_id','nodes','slots'
        }, 'Invalid source coverage inventory')
        require(inventory['contract'] == CONTRACT, 'Unsupported source coverage contract')
        require(isinstance(inventory['opt_digest'], str)
                and re.fullmatch('sha256:[0-9a-f]{64}', inventory['opt_digest']),
                'Coverage requires an exact OPT digest')
        require(inventory['template'] == template['template_name'],
                'Coverage inventory belongs to another template')
        require(isinstance(inventory['composition_archetype_id'], str)
                and inventory['composition_archetype_id'].startswith('openEHR-EHR-COMPOSITION.'),
                'Coverage requires a COMPOSITION archetype')
        nodes, slots = inventory['nodes'], inventory['slots']
        require(isinstance(nodes,list) and 0 < len(nodes) <= 200000,
                'Coverage inventory requires 1–200000 structural nodes')
        require(isinstance(slots,list) and len(slots) <= 10000, 'Too many slot declarations')
        plan = cls()
        plan.root = PathNode()
        plan.inventory = inventory
        selected = {b['path'] for g in template['groups'] for b in g['fields']}
        selected.update(b['path'] for b in template.get('activity_fields', []))
        selected_paths = {segments(p) for p in selected}
        selected_ancestors = {parts[:i] for parts in selected_paths
                              for i in range(1, len(parts) + 1)}
        seen = set()
        for row in nodes:
            require(isinstance(row,dict) and set(row)=={'path','rm_type'}, 'Invalid structural node')
            path, rm = row['path'], row['rm_type']
            require(isinstance(path,str) and (not path or path.startswith('/')) and path not in seen,
                    'Coverage paths must be unique composition paths')
            require(rm in STRUCTURAL_TYPES, 'Unsupported coverage RM type')
            seen.add(path)
            node = plan._node(segments(path))
            node.rm_type = rm
            node.selected = tuple(segments(path)) in selected_ancestors
        element_paths = {segments(row['path']) for row in nodes if row['rm_type'] == 'ELEMENT'}
        for parts in selected_paths:
            require(any(parts[:i] in element_paths for i in range(1, len(parts) + 1)),
                    'Inventory-bound fields must resolve to an explicit OPT ELEMENT')
        for group in template['groups']:
            require(group['anchor'] in seen, 'Fact anchor is absent from the OPT structure inventory')
        require(plan.root.rm_type == 'COMPOSITION', 'Coverage requires its explicit composition root')
        seen_slots = set()
        for slot in slots:
            require(isinstance(slot,dict) and set(slot)=={
                'path','rm_type','occurrences','closed','include_expressions','exclude_expressions'
            }, 'Invalid slot declaration')
            require(isinstance(slot['path'],str) and slot['path'].startswith('/')
                    and slot['path'] not in seen_slots, 'Invalid or duplicate slot path')
            require(isinstance(slot['rm_type'],str) and (slot['closed'] is None or type(slot['closed']) is bool)
                    and isinstance(slot['occurrences'],dict), 'Invalid slot type or occurrences')
            for k in ('include_expressions','exclude_expressions'):
                require(isinstance(slot[k],list) and all(isinstance(s,str) for s in slot[k]),
                        'Slot constraints must be source expressions')
            parts = segments(slot['path']);require(bool(parts),'Slot path cannot be empty')
            seen_slots.add(slot['path'])
            plan._node(parts[:-1]).slots.setdefault(parts[-1][0],[]).append(slot)
        return plan

    def _node(self, parts):
        node = self.root
        for attribute, selector in parts:
            identity = selector_parts(selector) if selector else (None,None)
            node = node.children.setdefault(attribute,{}).setdefault(identity,PathNode())
        return node

    def collector(self):
        return CoverageRun(self)


class CoverageRun:
    def __init__(self, plan):
        self.plan = plan
        self.initial = ({(None,None):plan.root}, ())
        self.counts = dict(observed_elements=0, selected_elements=0,
                           not_selected_elements=0, unmodelled_elements=0,
                           unexpected_structures=0)
        self.issues = []
        self.issue_count = 0

    def observe(self, node, cursor, location):
        candidates, slots = cursor
        node_id = node.get('archetype_node_id')
        name = node.get('name')
        name = name.get('value') if isinstance(name,dict) else None
        # Exact lookup avoids scanning every sibling constraint for every node.
        keys = [(None,None)]
        if isinstance(node_id,str):
            keys.append((node_id,None))
            if isinstance(name,str): keys.append((node_id,name))
        states = [candidates[k] for k in keys if k in candidates]
        rm = node.get('_type')
        # Canonical serializers can omit non-polymorphic RM type tags. Infer
        # only a unique type at an already matched, explicit OPT path.
        expected = {n.rm_type for n in states if n.rm_type}
        if rm is None and len(expected) == 1:
            rm = next(iter(expected))
        if not location:
            details = node.get('archetype_details',{})
            template = details.get('template_id',{}) if isinstance(details,dict) else {}
            if (node_id != self.plan.inventory['composition_archetype_id']
                    or not isinstance(template,dict)
                    or template.get('value') != self.plan.inventory['template']):
                self._issue('SOURCE_TEMPLATE_IDENTITY_MISMATCH',node,location,())
        known = any(n.rm_type == rm or (n.rm_type=='EVENT' and rm in {'POINT_EVENT','INTERVAL_EVENT'})
                    for n in states)
        if rm == 'ELEMENT':
            self.counts['observed_elements'] += 1
            self.counts['selected_elements' if known and any(n.selected for n in states)
                        else 'not_selected_elements' if known else 'unmodelled_elements'] += 1
        if (rm in STRUCTURAL_TYPES or isinstance(node_id, str) or any(n.rm_type for n in states)) and not known:
            self.counts['unexpected_structures'] += 1
            self._issue('UNMODELLED_SOURCE_STRUCTURE',node,location,slots)
        return states

    def descend(self, states, attribute):
        children, slots = {}, []
        for node in states:
            for identity, child in node.children.get(attribute,{}).items():
                # Overlapping wildcard/named paths are conservatively ambiguous.
                # Keep every possible path state via a small synthetic merged node.
                if identity in children and children[identity] is not child:
                    children[identity] = self._merge(children[identity],child)
                else: children[identity] = child
            slots.extend(node.slots.get(attribute,[]))
        return children, slots

    @staticmethod
    def _merge(a,b):
        # Duplicate predicates normally cannot occur in an exact OPT. Distinct
        # wildcard/named parent matches may converge; conflicting types fail closed.
        merged = PathNode(rm_type=a.rm_type if a.rm_type==b.rm_type else None,
                          selected=a.selected or b.selected)
        for parent in (a,b):
            for attr, edges in parent.children.items():
                dest=merged.children.setdefault(attr,{})
                for key,child in edges.items():
                    dest[key]=CoverageRun._merge(dest[key],child) if key in dest and dest[key] is not child else child
            for attr, slots in parent.slots.items():merged.slots.setdefault(attr,[]).extend(slots)
        return merged

    def _issue(self, code, node, location, slots):
        self.issue_count += 1
        if len(self.issues) >= MAX_ISSUES: return
        # Structural identifiers only: never copy names, values or free text.
        identifier=node.get('archetype_node_id')
        candidates=[s['path'] for s in slots if s['rm_type']==node.get('_type')]
        self.issues.append({'code':code,
            'location':'/'+'/'.join(str(p).replace('~','~0').replace('/','~1') for p in location),
            'rm_type':str(node.get('_type',''))[:80],
            'node_id':identifier[:256] if isinstance(identifier,str) else None,
            'candidate_slots':candidates[:3],
            'slot_validity':'not_evaluated'})

    def result(self):
        return {'contract':CONTRACT, 'opt_digest':self.plan.inventory['opt_digest'],
                'status':'unmodelled_content' if self.issue_count else 'covered',
                'scope':'composition_structures_and_elements', 'counts':dict(self.counts),
                'issue_count':self.issue_count, 'issues':self.issues,
                'issues_truncated':self.issue_count>len(self.issues),
                'conformance_validated':False}


def upgrade_rule(rule, opt_xml, *, epoch, effective_at):
    """Build a new inventory-bearing epoch while preserving mapping choices."""
    from .rules import RuleRevision, RuleSet
    from .default_mapping import _StructureCompiler
    previous=RuleRevision.compile(rule)
    require(type(epoch) is int and epoch>previous.epoch, 'Coverage changes require a new rule epoch')
    compiler=_StructureCompiler(opt_xml.encode() if isinstance(opt_xml,str) else opt_xml)
    semantic=compiler.compile()
    require(semantic.template.template_id==previous.template, 'OPT must match the exact rule template')
    value=deepcopy(previous.document());value.pop('_id',None);value.pop('revision',None)
    value.update(epoch=epoch,effective_at=effective_at)
    value['catalog']['templates'][0]['source_coverage']=inventory_from_compiler(compiler,semantic)
    candidate=RuleRevision.compile(value);RuleSet([previous,candidate])
    return candidate.document()
