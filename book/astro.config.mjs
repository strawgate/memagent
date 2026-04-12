import { defineConfig } from "astro/config";
import starlight from "@astrojs/starlight";

export default defineConfig({
  site: 'https://strawgate.github.io',
  base: '/memagent',
  integrations: [
    starlight({
      title: "logfwd",
      description: "Fast log forwarder with SQL transforms",
      social: [
        {
          icon: "github",
          label: "GitHub",
          href: "https://github.com/strawgate/memagent",
        },
      ],
      editLink: {
        baseUrl: "https://github.com/strawgate/memagent/edit/main/book/",
      },
      lastUpdated: true,
      customCss: ["./src/styles/custom.css"],
      sidebar: [
        {
          label: "Getting Started",
          items: [
            { label: "Choose the Right Guide", slug: "getting-started/which-guide" },
            { label: "Installation", slug: "getting-started/installation" },
            { label: "Quick Start", slug: "getting-started/quickstart" },
            { label: "Your First Pipeline", slug: "getting-started/first-pipeline" },
          ],
        },
        {
          label: "Configuration",
          items: [
            { label: "Config Reference", slug: "configuration/reference" },
            { label: "Input Types", slug: "configuration/inputs" },
            { label: "Output Types", slug: "configuration/outputs" },
            { label: "SQL Transforms", slug: "configuration/sql-transforms" },
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
          label: "Collector University",
          items: [
            { label: "How logfwd Works", slug: "architecture" },
            { label: "Why Tailing Is Hard", slug: "architecture/tailing" },
            { label: "Why Columnar Storage Matters", slug: "architecture/columnar" },
            { label: "Scanner Deep Dive", slug: "architecture/scanner" },
            { label: "Pipeline Design", slug: "architecture/pipeline" },
            { label: "Performance", slug: "architecture/performance" },
          ],
        },
        {
          label: "Troubleshooting",
          slug: "troubleshooting",
        },
        {
          label: "Development",
          items: [
            { label: "Contributing", slug: "development/contributing" },
            { label: "Building & Testing", slug: "development/building" },
            { label: "Benchmarking", slug: "development/benchmarking" },
          ],
        },
      ],
    }),
  ],
});
