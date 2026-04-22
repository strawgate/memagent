import { defineConfig } from "astro/config";
import starlight from "@astrojs/starlight";

export default defineConfig({
  site: 'https://strawgate.github.io',
  base: '/fastforward',
  redirects: {
    '/how-it-works/':             '/learn/',
    '/how-it-works/backpressure/': '/learn/backpressure/',
    '/how-it-works/checkpoints/':  '/learn/checkpoints/',
    '/how-it-works/columnar/':     '/learn/columnar/',
    '/how-it-works/performance/':  '/learn/performance/',
    '/how-it-works/scanner/':      '/learn/scanner/',
    '/how-it-works/tailing/':      '/learn/tailing/',
  },
  integrations: [
    starlight({
      title: "FastForward",
      description: "a learning-oriented log forwarder built with Rust",
      social: [
        {
          icon: "github",
          label: "GitHub",
          href: "https://github.com/strawgate/fastforward",
        },
      ],
      editLink: {
        baseUrl: "https://github.com/strawgate/fastforward/edit/main/book/",
      },
      lastUpdated: true,
      customCss: ["./src/styles/custom.css"],
      sidebar: [
        {
          label: "Quick Start",
          slug: "quick-start",
        },
        {
          label: "Learn",
          items: [
            { label: "Overview", slug: "learn" },
            { label: "Inputs", slug: "learn/inputs" },
            { label: "Tailing", slug: "learn/tailing" },
            { label: "Scanner Deep Dive", slug: "learn/scanner" },
            { label: "Columnar", slug: "learn/columnar" },
            { label: "Backpressure in Action", slug: "learn/backpressure" },
            { label: "Checkpoint Ordering", slug: "learn/checkpoints" },
            { label: "Performance", slug: "learn/performance" },
          ],
        },
        {
          label: "Configuration",
          items: [
            { label: "Config Builder", slug: "configuration/config-builder" },
            { label: "SQL Transforms", slug: "configuration/sql-transforms" },
            { label: "Input Types", slug: "configuration/inputs" },
            { label: "Output Types", slug: "configuration/outputs" },
            { label: "YAML Reference", slug: "configuration/reference" },
          ],
        },
        {
          label: "Deployment",
          items: [
            { label: "Kubernetes DaemonSet", slug: "deployment/kubernetes" },
            { label: "Docker", slug: "deployment/docker" },
            { label: "Monitoring & Diagnostics", slug: "deployment/monitoring" },
          ],
        },
        {
          label: "Troubleshooting",
          slug: "troubleshooting",
        },
      ],
    }),
  ],
});
