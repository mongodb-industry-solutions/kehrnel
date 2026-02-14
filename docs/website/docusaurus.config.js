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
  onBrokenMarkdownLinks: 'warn',

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
