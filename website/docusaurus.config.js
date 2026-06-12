// @ts-check
const { themes: prismThemes } = require('prism-react-renderer');

const config = {
  title: 'Retail Fabric RTI Demo',
  tagline: 'Microsoft Fabric Real-Time Intelligence demo for retail',
  url: 'https://amattas.github.io',
  baseUrl: '/retail-demo/',
  organizationName: 'amattas',
  projectName: 'retail-demo',
  onBrokenLinks: 'throw',
  favicon: 'img/favicon.svg',
  // .md files parse as CommonMark (migrated from MkDocs); .mdx files get MDX.
  markdown: { format: 'detect' },
  themeConfig: {
    // No defaultMode: follow the visitor's OS/browser preference. (A manual
    // toggle persists in localStorage and overrides this — by design.)
    colorMode: { respectPrefersColorScheme: true },
    // Docusaurus's default prism theme (palenight) is a dark-background token
    // palette in BOTH modes — unreadable on light panels. Per-mode themes:
    prism: {
      theme: prismThemes.github,
      darkTheme: prismThemes.nightOwl,
      additionalLanguages: ['bash', 'python', 'sql', 'json', 'yaml'],
    },
    navbar: {
      title: 'Retail Fabric RTI',
      logo: { src: 'img/logo.svg', width: 26, height: 26 },
      items: [
        { to: '/setup/', label: 'Setup', position: 'left' },
        { to: '/architecture/', label: 'Architecture', position: 'left' },
        { to: '/datagen/', label: 'Data Generator', position: 'left' },
        { to: '/fabric/', label: 'Fabric', position: 'left' },
        {
          href: 'https://github.com/amattas/retail-demo',
          label: 'GitHub',
          position: 'right',
        },
      ],
    },
  },
  presets: [
    [
      '@docusaurus/preset-classic',
      {
        docs: {
          routeBasePath: '/',
          sidebarPath: './sidebars.js',
        },
        blog: false,
        theme: { customCss: './src/css/custom.css' },
        sitemap: { lastmod: 'date', changefreq: 'weekly', priority: 0.5, filename: 'sitemap.xml' },
      },
    ],
  ],
  themes: [
    [
      // Self-hosted full-text search — no external service, indexed at build time.
      require.resolve('@easyops-cn/docusaurus-search-local'),
      {
        hashed: true,
        docsRouteBasePath: '/',
        indexBlog: false,
        highlightSearchTermsOnTargetPage: true,
      },
    ],
  ],
};
module.exports = config;
