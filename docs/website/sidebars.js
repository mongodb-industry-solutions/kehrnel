/** @type {import('@docusaurus/plugin-content-docs').SidebarsConfig} */
const sidebars = {
  tutorialSidebar: [
    'intro',
    {
      type: 'category',
      label: 'Vision',
      items: [
        'vision/roadmap',
        'vision/licensing',
      ],
    },
    {
      type: 'category',
      label: 'Getting Started',
      items: [
        'getting-started/installation',
        'getting-started/quickstart',
        'getting-started/configuration',
      ],
    },
    {
      type: 'category',
      label: 'CLI Reference',
      items: [
        'cli/overview',
        'cli/api-server',
        'cli/transform',
        'cli/validate',
        'cli/generate',
        'cli/ingest',
        'cli/mapping',
        'cli/bundles',
        'cli/validate-pack',
      ],
    },
    {
      type: 'category',
      label: 'API Reference',
      items: [
        'api/overview',
        'api/endpoint-catalog',
        'api/endpoints/fhir-preview',
        'api/endpoints/openehr-templates',
        'api/endpoints/openehr-ehr',
        'api/endpoints/openehr-composition-directory',
        'api/endpoints/openehr-versioning',
        'api/endpoints/openehr-query',
        'api/endpoints/strategy-config-and-ingest',
        'api/endpoints/strategy-synthetic-and-jobs',
        'api/endpoints/strategy-registry',
        'api/core-openapi',
        'api/domain-openehr',
        'api/domain-fhir-preview',
        'api/strategy-runtime',
        'api/admin-environments',
      ],
    },
    {
      type: 'category',
      label: 'Strategies',
      items: [
        'strategies/overview',
        'strategies/status-and-roadmap',
        {
          type: 'category',
          label: 'openEHR RPS Dual',
          items: [
            'strategies/openehr-rps-dual/introduction',
            'strategies/openehr-rps-dual/data-model',
            'strategies/openehr-rps-dual/query-translation',
            'strategies/openehr-rps-dual/configuration',
          ],
        },
      ],
    },
    {
      type: 'category',
      label: 'Concepts',
      items: [
        'concepts/data-sampling',
        'concepts/reversed-paths',
        'concepts/dual-collection',
        'concepts/aql-to-mql',
        'concepts/flattening',
      ],
    },
    {
      type: 'category',
      label: 'Architecture',
      items: [
        'architecture/overview',
        'architecture/transformation-pipeline',
        'architecture/query-engine',
      ],
    },
    {
      type: 'category',
      label: 'Deployment',
      items: [
        'deployment/docker',
        'deployment/atlas',
        'deployment/production',
      ],
    },
  ],
};

export default sidebars;
