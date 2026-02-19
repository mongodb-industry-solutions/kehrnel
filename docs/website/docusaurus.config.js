// @ts-check
import {themes as prismThemes} from 'prism-react-renderer';

/** @type {import('@docusaurus/types').Config} */
const config = {
  title: '{kehrnel}',
  tagline: 'Multi-strategy healthcare data runtime',
  favicon: 'img/favicon.png',

  url: 'https://mongodb-industry-solutions.github.io',
  baseUrl: '/guide/',

  organizationName: 'mongodb-industry-solutions',
  projectName: 'kehrnel',

  onBrokenLinks: 'throw',
  markdown: {
    hooks: {
      onBrokenMarkdownLinks: 'warn',
    },
  },

  i18n: {
    defaultLocale: 'en',
    locales: ['en'],
  },

  presets: [
    [
      'classic',
      /** @type {import('@docusaurus/preset-classic').Options} */
      ({
        docs: {
          sidebarPath: './sidebars.js',
          editUrl: 'https://github.com/mongodb-industry-solutions/kehrnel/tree/main/docs/website/',
        },
        theme: {
          customCss: './src/css/custom.css',
        },
      }),
    ],
  ],

  plugins: [
    // Local dev convenience:
    // - Docusaurus runs on its own port (default 8001)
    // - kehrnel API runs separately (commonly 8000)
    // Proxy API routes so CTA links like /docs and /redoc work during `npm start`.
    function kehrnelDevProxyPlugin() {
      return {
        name: 'kehrnel-dev-proxy',
        configureWebpack(_webpackConfig, isServer) {
          if (isServer) return {};
          const apiTarget = process.env.KEHRNEL_API_ORIGIN || 'http://localhost:8000';
          return {
            devServer: {
              proxy: {
                '/docs': {target: apiTarget, changeOrigin: true},
                '/redoc': {target: apiTarget, changeOrigin: true},
                '/openapi.json': {target: apiTarget, changeOrigin: true},
                '/openapi': {target: apiTarget, changeOrigin: true},
                '/health': {target: apiTarget, changeOrigin: true},
                '/api': {target: apiTarget, changeOrigin: true},
                '/environments': {target: apiTarget, changeOrigin: true},
                '/strategies': {target: apiTarget, changeOrigin: true},
                '/ops': {target: apiTarget, changeOrigin: true},
                '/bundles': {target: apiTarget, changeOrigin: true},
              },
            },
          };
        },
      };
    },
  ],

  themeConfig:
    /** @type {import('@docusaurus/preset-classic').ThemeConfig} */
    ({
      image: 'img/kehrnel-social-card.png',
      navbar: {
        title: '',
        logo: {
          alt: 'Kehrnel',
          src: 'img/logo.svg',
        },
        items: [
          {
            type: 'docSidebar',
            sidebarId: 'tutorialSidebar',
            position: 'left',
            label: 'Documentation',
          },
          {
            href: 'https://github.com/mongodb-industry-solutions/kehrnel',
            label: 'GitHub',
            position: 'right',
          },
        ],
      },
      footer: {
        style: 'dark',
        links: [
          {
            title: 'Docs',
            items: [
              {
                label: 'Getting Started',
                to: '/docs/getting-started/installation',
              },
              {
                label: 'CLI Reference',
                to: '/docs/cli/overview',
              },
              {
                label: 'API Reference',
                to: '/docs/api/overview',
              },
            ],
          },
          {
            title: 'Resources',
            items: [
              {
                label: 'openEHR Specification',
                href: 'https://specifications.openehr.org/',
              },
              {
                label: 'MongoDB Atlas',
                href: 'https://www.mongodb.com/atlas',
              },
            ],
          },
          {
            title: 'More',
            items: [
              {
                label: 'GitHub',
                href: 'https://github.com/mongodb-industry-solutions/kehrnel',
              },
            ],
          },
        ],
        copyright: `Copyright © ${new Date().getFullYear()} MongoDB, Inc. Built with Docusaurus.`,
      },
      prism: {
        theme: prismThemes.github,
        darkTheme: prismThemes.dracula,
        additionalLanguages: ['bash', 'json', 'python', 'sql'],
      },
    }),

};

export default config;
