import { defineConfig } from 'vitepress'

export default defineConfig({
  base: '/sage/',
  title: "Sage",
  description: "A Redis & Valkey client for Scala 3",
  head: [
    ['link', { rel: 'icon', href: '/sage/favicon.png' }],
    ['script', { defer: '', src: 'https://cloud.umami.is/script.js', 'data-website-id': '2ba71608-2c44-4547-804e-ed78f3a41ad1' }]
  ],

  themeConfig: {
    nav: [
      { text: 'Home', link: '/' },
      { text: 'Documentation', link: '/getting-started' },
      { text: 'FAQ', link: '/faq' },
      { text: 'About', link: '/about' },
    ],

    logo: { light: '/sage.svg', dark: '/sage-dark.svg' },

    sidebar: [
      {
        text: 'Documentation',
        items: [
          { text: 'Getting started', link: '/getting-started' },
          { text: 'Commands & codecs', link: '/commands' },
          { text: 'Pipelines & transactions', link: '/pipelines-transactions' },
          { text: 'Pub/Sub', link: '/pubsub' },
          { text: 'Streams', link: '/streams' },
          { text: 'JSON', link: '/json' },
          { text: 'Client-side caching', link: '/client-side-caching' },
          { text: 'Configuration', link: '/configuration' },
          { text: 'Error handling', link: '/error-handling' },
          { text: 'Observability', link: '/observability' },
        ]
      },
      {
        text: 'FAQ', link: '/faq'
      },
      {
        text: 'About', link: '/about'
      }
    ],

    socialLinks: [
      { icon: 'github', link: 'https://github.com/ghostdogpr/sage' }
    ],

    search: {
      provider: 'local'
    }
  }
})
