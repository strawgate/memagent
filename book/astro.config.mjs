import { defineConfig } from "astro/config";
import starlight from "@astrojs/starlight";

export default defineConfig({
  site: 'https://strawgate.github.io',
  base: '/memagent',
  integrations: [
    starlight({
      title: "logfwd",
      description: "Tail log files. Transform with SQL. Ship anywhere.",
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
          label: "Quick Start",
          slug: "quick-start",
        },
        {
          label: "How It Works",
          items: [
            { label: "Pipeline Explorer", slug: "how-it-works" },
            { label: "Scanner Deep Dive", slug: "how-it-works/scanner" },
            { label: "Why Tailing Is Hard", slug: "how-it-works/tailing" },
            { label: "Why Columnar Matters", slug: "how-it-works/columnar" },
            { label: "Performance", slug: "how-it-works/performance" },
          ],
        },
        {
          label: "Configuration",
          items: [
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
        {
          label: "Development",
          items: [
            { label: "Contributing", slug: "development/contributing" },
            { label: "Benchmarking", slug: "development/benchmarking" },
          ],
        },
      ],
    }),
  ],
});
