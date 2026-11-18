import { defineConfig } from 'vitepress'

const description = "A Redis & Valkey client for Scala 3"

export default defineConfig({
  base: '/sage/',
  title: "Sage",
  description,
  head: [
    ['link', { rel: 'icon', href: '/sage/favicon.png' }],
    ['script', { defer: '', src: 'https://cloud.umami.is/script.js', 'data-website-id': '2ba71608-2c44-4547-804e-ed78f3a41ad1' }],
    // VitePress does not add `base` to head entries, so these URLs are absolute.
    ['meta', { property: 'og:type', content: 'website' }],
    ['meta', { property: 'og:url', content: 'https://ghostdogpr.github.io/sage/' }],
    ['meta', { property: 'og:title', content: 'Sage' }],
    ['meta', { property: 'og:description', content: description }],
    ['meta', { property: 'og:image', content: 'https://ghostdogpr.github.io/sage/og.png' }],
    ['meta', { property: 'og:image:width', content: '1200' }],
    ['meta', { property: 'og:image:height', content: '630' }],
    ['meta', { property: 'og:image:alt', content: 'Sage logo (glasses with Scala bars in the lenses) above the title Sage: A Redis & Valkey client for Scala 3' }],
    ['meta', { name: 'twitter:card', content: 'summary_large_image' }]
  ],

  themeConfig: {
    nav: [
      { text: 'Home', link: '/' },
      { text: 'Documentation', link: '/getting-started' },
      { text: 'FAQ', link: '/faq' },
      { text: 'About', link: '/about' },
      { text: 'Scaladoc', link: 'https://javadoc.io/doc/com.github.ghostdogpr/sage-core_3/' },
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
          { text: 'Rate limiting', link: '/rate-limiting' },
          { text: 'Distributed locks', link: '/distributed-locks' },
          { text: 'Configuration', link: '/configuration' },
          { text: 'Error handling', link: '/error-handling' },
          { text: 'Observability', link: '/observability' },
          { text: 'Testing', link: '/testing' },
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
