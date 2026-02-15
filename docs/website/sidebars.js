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
        'cli/auth-context',
        'cli/core',
        'cli/common',
        'cli/domains',
        'cli/strategies',
      ],
    },
    {
      type: 'category',
      label: 'API Reference',
      items: [
        'api/overview',
        'api/layers',
        'api/core-openapi',
        'api/common-api',
        'api/domain-openehr',
        'api/strategy-runtime',
      ],
    },
    {
      type: 'category',
      label: 'Domains',
      items: [
        'domains/index',
        {
          type: 'category',
          label: 'openEHR',
          items: [
            'domains/openehr/index',
            'strategies/openehr-rps-dual/introduction',
            'strategies/openehr-rps-dual/cli-workflows',
            'strategies/openehr-rps-dual/data-model',
            'strategies/openehr-rps-dual/query-translation',
            'strategies/openehr-rps-dual/configuration',
          ],
        },
      ],
    },
    {
      type: 'category',
      label: 'Strategies',
      items: [
        'strategies/overview',
        'strategies/status-and-roadmap',
      ],
    },
    {
      type: 'category',
      label: 'Concepts',
      items: [
        'concepts/overview',
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
