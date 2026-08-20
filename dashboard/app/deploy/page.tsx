import type { Metadata } from "next";
import Link from "next/link";
import {
  Rocket,
  ExternalLink,
  Zap,
  Server,
  ShieldCheck,
  Clock,
  GitBranch,
  ListChecks,
  Webhook,
  Lock,
  FolderTree,
  Terminal,
  ChevronRight,
} from "lucide-react";
import { GithubMark } from "@/components/icons/GithubMark";
import { Reveal } from "@/components/ui/Reveal";
import { CodeBlock } from "@/components/deploy/CodeBlock";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/Card";
import { Badge } from "@/components/ui/Badge";

const REPO_URL = "https://github.com/sk7-dev/oss-job-post-bot";

export const metadata: Metadata = {
  title: "Deploy It Yourself · Job Watcher",
  description: "Run your own copy of the job watcher bot for free on GitHub Actions.",
};

const FEATURES = [
  {
    icon: Server,
    title: "Five source types, out of the box",
    description: "Greenhouse, Lever, Ashby, Workday, and ADP career boards — no custom scraping required.",
  },
  {
    icon: ListChecks,
    title: "Keyword & location filters",
    description: "Match on title keywords and locations, and drop anything that hits an excluded keyword.",
  },
  {
    icon: Clock,
    title: "Dedupe built in",
    description: "Already-seen jobs are tracked in state_seen.json, so you're alerted once per posting, ever.",
  },
  {
    icon: Webhook,
    title: "Discord alerts",
    description: "New matches are pushed straight to a Discord channel via webhook — no polling required.",
  },
  {
    icon: GitBranch,
    title: "Runs on GitHub Actions",
    description: "A scheduled workflow runs the scan and commits its own state back to the repo. No server to babysit.",
  },
  {
    icon: Lock,
    title: "Your data, your repo",
    description: "No accounts, no third-party storage. Everything lives in the repository you control.",
  },
];

const SOURCE_TYPES = ["Greenhouse", "Lever", "Ashby", "Workday", "ADP"];

const REPO_LAYOUT = `watcher.py               # main bot logic: fetch, filter, dedupe, alert
config.json               # YOUR sources + filters (sample data, replace)
state_seen.json           # job IDs already seen (starts empty)
.github/workflows/        # scheduled GitHub Action that runs the bot`;

const QUICK_START_STEPS = [
  {
    title: "Fork or clone the repo",
    body: "Grab your own copy to work from.",
  },
  {
    title: "Install dependencies",
    body: "Everything the bot needs is in requirements.txt.",
    code: "pip install -r requirements.txt",
  },
  {
    title: "Edit config.json",
    body: "It ships with 3 working sample sources (GitLab/Greenhouse, Notion/Ashby, Salesforce/Workday) so you can run it immediately and see how the format works — swap them for companies you actually care about.",
  },
  {
    title: "Set DISCORD_WEBHOOK_URL (optional)",
    body: "Without it, the bot still runs, prints results, and updates state_seen.json; it just skips sending alerts.",
  },
  {
    title: "Run it",
    body: "Run it again and you'll see New jobs: 0 for anything already recorded — that's the dedupe working.",
    code: "python watcher.py",
  },
];

const FILTER_FIELDS = [
  {
    name: "title_keywords_any",
    description: "A job matches if its title contains at least one of these (case-insensitive substring match).",
  },
  {
    name: "locations_any",
    description: "A job matches if its location contains at least one of these.",
  },
  {
    name: "excluded_keywords_any",
    description: "A job is dropped if its title, location, or department contains any of these.",
  },
];

const SOURCE_CONFIG_ROWS = [
  {
    type: "greenhouse",
    fields: "board_token",
    notes: "Find it in the company's Greenhouse job board URL: boards.greenhouse.io/<board_token>",
  },
  {
    type: "lever",
    fields: "company",
    notes: "From jobs.lever.co/<company>",
  },
  {
    type: "ashby",
    fields: "organization_key",
    notes: "From jobs.ashbyhq.com/<organization_key>",
  },
  {
    type: "workday",
    fields: "url (or tenant + site + base_url), optional search_text, limit",
    notes: "url is the public careers URL, e.g. https://<tenant>.wd#.myworkdayjobs.com/<site>",
  },
  {
    type: "adp",
    fields: "domain",
    notes: "ADP career site subdomain",
  },
];

const DISCORD_STEPS = [
  "In Discord, go to the target channel's Settings → Integrations → Webhooks → New Webhook, then copy its URL.",
  "Set it as DISCORD_WEBHOOK_URL, either as a local environment variable for testing, or as a GitHub Actions secret for the deployed bot.",
];

const DEPLOY_STEPS = [
  "Push this repo to your own GitHub repository.",
  "In the repo, go to Settings → Secrets and variables → Actions and add a secret named DISCORD_WEBHOOK_URL (skip this if you don't want Discord alerts — the workflow runs fine without it).",
  'Make sure Settings → Actions → General → Workflow permissions is set to "Read and write permissions" — the workflow commits state back to the repo.',
  "That's it. The workflow starts running on its schedule, or trigger it manually from the Actions tab (workflow_dispatch).",
];

export default function DeployPage() {
  return (
    <div className="space-y-10 pb-6 sm:space-y-14">
      {/* ===== Hero ===== */}
      <div className="relative overflow-hidden rounded-[20px] bg-slate-900 px-5 py-10 shadow-[0_20px_50px_rgba(15,23,42,0.35)] sm:px-10 sm:py-16">
        <div
          aria-hidden
          className="pointer-events-none absolute -left-24 -top-24 size-72 rounded-full bg-[var(--accent)]/30 blur-[90px] animate-[float-orb_16s_ease-in-out_infinite]"
        />
        <div
          aria-hidden
          className="pointer-events-none absolute -bottom-32 -right-16 size-80 rounded-full bg-violet-500/25 blur-[100px] animate-[float-orb-alt_18s_ease-in-out_infinite]"
        />
        <div
          aria-hidden
          className="pointer-events-none absolute inset-0 bg-[radial-gradient(circle_at_top,rgba(255,255,255,0.08),transparent_60%)]"
        />

        <div className="relative mx-auto max-w-2xl text-center">
          <span className="inline-flex items-center gap-1.5 rounded-full border border-white/15 bg-white/5 px-3 py-1 text-xs font-medium text-slate-200 backdrop-blur-sm">
            <Rocket className="size-3.5 text-[var(--accent)]" aria-hidden />
            Open source · runs free on GitHub Actions
          </span>

          <h1 className="mt-5 bg-gradient-to-r from-white via-blue-100 to-white bg-[length:200%_auto] bg-clip-text text-[32px] font-bold leading-tight tracking-tight text-transparent animate-[gradient-x_6s_ease_infinite] sm:text-[44px]">
            Deploy it yourself
          </h1>
          <p className="mx-auto mt-4 max-w-xl text-[15px] leading-relaxed text-slate-300 sm:text-base">
            This dashboard is powered by a lightweight, open-source Python bot that watches career sites, filters
            matching roles, and sends alerts straight to Discord — running for free on a schedule via GitHub
            Actions. Fork it, point it at the companies you care about, and set it running.
          </p>

          <div className="mt-8 flex flex-col items-center justify-center gap-3 sm:flex-row">
            <a
              href={REPO_URL}
              target="_blank"
              rel="noopener noreferrer"
              className="group inline-flex w-full items-center justify-center gap-2 rounded-lg bg-white px-5 py-2.5 text-sm font-semibold text-slate-900 shadow-[0_1px_2px_rgba(0,0,0,0.06)] transition-all hover:-translate-y-0.5 hover:shadow-[0_10px_24px_rgba(255,255,255,0.15)] sm:w-auto"
            >
              <GithubMark className="size-4" />
              View on GitHub
              <ChevronRight className="size-3.5 shrink-0 opacity-60 transition-transform group-hover:translate-x-0.5" aria-hidden />
            </a>
            <a
              href="#quick-start"
              className="inline-flex w-full items-center justify-center gap-1.5 rounded-lg border border-white/15 bg-white/5 px-5 py-2.5 text-sm font-medium text-slate-200 backdrop-blur-sm transition-colors hover:bg-white/10 sm:w-auto"
            >
              Jump to quick start
            </a>
          </div>

          <p className="mt-5 truncate font-mono text-xs text-slate-500">github.com/sk7-dev/oss-job-post-bot</p>
        </div>
      </div>

      {/* ===== What it does ===== */}
      <section>
        <Reveal>
          <h2 className="text-[22px] font-bold text-[var(--text-primary)] sm:text-[26px]">What it does</h2>
          <p className="mt-1.5 max-w-2xl text-sm text-slate-500">
            A clean template with no personal job search data, filters, or secrets baked in.
          </p>
        </Reveal>

        <div className="mt-5 grid grid-cols-1 gap-3.5 sm:grid-cols-2 lg:grid-cols-3">
          {FEATURES.map((feature, i) => (
            <Reveal key={feature.title} delay={i * 60}>
              <Card className="group h-full transition-all duration-300 hover:-translate-y-0.5 hover:shadow-[0_8px_24px_rgba(15,23,42,0.08)]">
                <CardContent className="pt-3.5 sm:pt-5">
                  <span className="flex size-9 items-center justify-center rounded-lg bg-[var(--accent-soft)] text-[var(--accent)] transition-transform duration-300 group-hover:scale-105">
                    <feature.icon className="size-4.5" aria-hidden />
                  </span>
                  <h3 className="mt-3 text-[15px] font-semibold text-[var(--text-primary)]">{feature.title}</h3>
                  <p className="mt-1.5 text-[13px] leading-relaxed text-slate-500">{feature.description}</p>
                </CardContent>
              </Card>
            </Reveal>
          ))}
        </div>

        <Reveal delay={120} className="mt-4 flex flex-wrap items-center gap-2">
          <span className="text-xs font-medium text-slate-400">Supported sources:</span>
          {SOURCE_TYPES.map((type) => (
            <Badge key={type} tone="blue">
              {type}
            </Badge>
          ))}
        </Reveal>
      </section>

      {/* ===== Repo layout ===== */}
      <section>
        <Reveal>
          <Card className="overflow-hidden">
            <CardHeader>
              <CardTitle className="flex items-center gap-2 text-[16px] sm:text-[18px]">
                <FolderTree className="size-4 text-slate-400" aria-hidden />
                Repo layout
              </CardTitle>
            </CardHeader>
            <CardContent>
              <CodeBlock code={REPO_LAYOUT} />
            </CardContent>
          </Card>
        </Reveal>
      </section>

      {/* ===== Quick start ===== */}
      <section id="quick-start" className="scroll-mt-20">
        <Reveal>
          <h2 className="flex items-center gap-2 text-[22px] font-bold text-[var(--text-primary)] sm:text-[26px]">
            <Terminal className="size-5 text-[var(--accent)]" aria-hidden />
            Quick start
          </h2>
          <p className="mt-1.5 max-w-2xl text-sm text-slate-500">Get it running locally in five steps.</p>
        </Reveal>

        <ol className="mt-5 space-y-3">
          {QUICK_START_STEPS.map((step, i) => (
            <Reveal key={step.title} delay={i * 70}>
              <li className="relative flex gap-4 rounded-[14px] border border-[var(--border-subtle)] bg-[var(--surface)] p-4 shadow-[0_1px_2px_rgba(15,23,42,0.04)] sm:p-5">
                <span className="flex size-7 shrink-0 items-center justify-center rounded-full bg-[var(--accent)] text-xs font-bold text-white">
                  {i + 1}
                </span>
                <div className="min-w-0 flex-1">
                  <p className="font-semibold text-[var(--text-primary)]">{step.title}</p>
                  <p className="mt-1 text-[13px] leading-relaxed text-slate-500">{step.body}</p>
                  {step.code ? <CodeBlock code={step.code} className="mt-3" /> : null}
                </div>
              </li>
            </Reveal>
          ))}
        </ol>
      </section>

      {/* ===== Configuring sources & filters ===== */}
      <section>
        <Reveal>
          <h2 className="text-[22px] font-bold text-[var(--text-primary)] sm:text-[26px]">Configuring sources & filters</h2>
          <p className="mt-1.5 max-w-2xl text-sm text-slate-500">
            <code className="font-mono text-xs">config.json</code> has two sections: filters, applied to every
            source, and sources, the list of career sites to watch.
          </p>
        </Reveal>

        <div className="mt-5 grid grid-cols-1 gap-4 lg:grid-cols-[0.85fr_1.15fr]">
          <Reveal>
            <Card className="h-full">
              <CardHeader>
                <CardTitle className="text-[16px] sm:text-[18px]">Filters</CardTitle>
              </CardHeader>
              <CardContent className="space-y-3.5">
                {FILTER_FIELDS.map((field) => (
                  <div key={field.name}>
                    <code className="rounded-md bg-slate-100 px-1.5 py-0.5 font-mono text-xs text-slate-700">
                      {field.name}
                    </code>
                    <p className="mt-1.5 text-[13px] leading-relaxed text-slate-500">{field.description}</p>
                  </div>
                ))}
                <p className="rounded-lg bg-slate-50 px-3 py-2 text-[12px] text-slate-500">
                  Leave a list empty (<code className="font-mono">[]</code>) to skip that filter entirely.
                </p>
              </CardContent>
            </Card>
          </Reveal>

          <Reveal delay={80}>
            <Card className="h-full overflow-hidden">
              <CardHeader>
                <CardTitle className="text-[16px] sm:text-[18px]">Source types</CardTitle>
              </CardHeader>
              <CardContent className="!px-0 !pb-0">
                <div className="overflow-x-auto">
                  <table className="w-full min-w-[560px] border-collapse text-left text-[13px]">
                    <thead>
                      <tr className="border-y border-[var(--border-subtle)] bg-slate-50 text-xs font-medium text-slate-500">
                        <th className="px-3.5 py-2.5 sm:px-6">Type</th>
                        <th className="px-3.5 py-2.5">Required fields</th>
                        <th className="px-3.5 py-2.5 sm:px-6">Notes</th>
                      </tr>
                    </thead>
                    <tbody className="divide-y divide-slate-100">
                      {SOURCE_CONFIG_ROWS.map((row) => (
                        <tr key={row.type} className="align-top">
                          <td className="px-3.5 py-3 font-mono text-xs font-medium text-[var(--accent)] sm:px-6">
                            {row.type}
                          </td>
                          <td className="px-3.5 py-3 font-mono text-xs text-slate-600">{row.fields}</td>
                          <td className="px-3.5 py-3 text-slate-500 sm:px-6">{row.notes}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
                <p className="px-3.5 py-3.5 text-[12px] text-slate-400 sm:px-6">
                  Open a company&rsquo;s careers page and watch the Network tab, or just read the URL structure —
                  most of these fields come straight from the public careers URL.
                </p>
              </CardContent>
            </Card>
          </Reveal>
        </div>
      </section>

      {/* ===== Discord + Deploy ===== */}
      <div className="grid grid-cols-1 gap-4 lg:grid-cols-2">
        <Reveal>
          <Card className="h-full">
            <CardHeader>
              <CardTitle className="flex items-center gap-2 text-[16px] sm:text-[18px]">
                <Webhook className="size-4 text-slate-400" aria-hidden />
                Setting up Discord alerts
              </CardTitle>
            </CardHeader>
            <CardContent>
              <ol className="space-y-3">
                {DISCORD_STEPS.map((step, i) => (
                  <li key={step} className="flex gap-3 text-[13px] leading-relaxed text-slate-600">
                    <span className="flex size-5 shrink-0 items-center justify-center rounded-full bg-slate-100 text-[11px] font-semibold text-slate-500">
                      {i + 1}
                    </span>
                    {step}
                  </li>
                ))}
              </ol>
            </CardContent>
          </Card>
        </Reveal>

        <Reveal delay={80}>
          <Card className="h-full border-[var(--accent)]/20">
            <CardHeader>
              <CardTitle className="flex items-center gap-2 text-[16px] sm:text-[18px]">
                <Zap className="size-4 text-[var(--accent)]" aria-hidden />
                Deploying the scheduled bot
              </CardTitle>
            </CardHeader>
            <CardContent>
              <p className="mb-3 text-[13px] leading-relaxed text-slate-500">
                The included workflow (<code className="font-mono text-xs">.github/workflows/watch-jobs.yml</code>)
                runs the bot every 10 minutes and commits its state back to the repo — no server of your own
                required.
              </p>
              <ol className="space-y-3">
                {DEPLOY_STEPS.map((step, i) => (
                  <li key={step} className="flex gap-3 text-[13px] leading-relaxed text-slate-600">
                    <span className="flex size-5 shrink-0 items-center justify-center rounded-full bg-[var(--accent-soft)] text-[11px] font-semibold text-[var(--accent)]">
                      {i + 1}
                    </span>
                    {step}
                  </li>
                ))}
              </ol>
            </CardContent>
          </Card>
        </Reveal>
      </div>

      {/* ===== Security notes ===== */}
      <section>
        <Reveal>
          <div className="flex items-start gap-3 rounded-[14px] border border-amber-200 bg-[var(--warning-soft)] px-4 py-4 sm:px-6">
            <ShieldCheck className="mt-0.5 size-5 shrink-0 text-[var(--warning)]" aria-hidden />
            <div className="space-y-1.5 text-[13px] leading-relaxed text-amber-900">
              <p className="text-sm font-semibold text-[var(--text-primary)]">Security notes</p>
              <p>
                <code className="font-mono text-xs">DISCORD_WEBHOOK_URL</code> should live only in GitHub Actions
                secrets — never in <code className="font-mono text-xs">config.json</code> or committed anywhere.
              </p>
              <p>
                <code className="font-mono text-xs">state_seen.json</code> fills up with real job posting data
                (titles, companies, URLs) as the bot runs. It&rsquo;s not sensitive on its own, but it&rsquo;s your
                data — keep the repo private if you&rsquo;d rather not share which jobs you&rsquo;re tracking.
              </p>
            </div>
          </div>
        </Reveal>
      </section>

      {/* ===== Final CTA ===== */}
      <section>
        <Reveal>
          <div className="relative overflow-hidden rounded-[20px] border border-[var(--border-subtle)] bg-gradient-to-br from-slate-50 to-white px-5 py-9 text-center sm:px-10 sm:py-12">
            <h2 className="text-[22px] font-bold text-[var(--text-primary)] sm:text-[26px]">Ready to run your own?</h2>
            <p className="mx-auto mt-2 max-w-md text-sm text-slate-500">
              Fork the repo, drop in your own sources and filters, and let GitHub Actions do the rest.
            </p>
            <div className="mt-6 flex flex-col items-center justify-center gap-3 sm:flex-row">
              <a
                href={REPO_URL}
                target="_blank"
                rel="noopener noreferrer"
                className="inline-flex w-full items-center justify-center gap-2 rounded-lg bg-[var(--accent)] px-5 py-2.5 text-sm font-semibold text-white shadow-[0_4px_14px_rgba(37,99,235,0.3)] transition-all hover:-translate-y-0.5 hover:bg-[var(--accent-hover)] hover:shadow-[0_8px_20px_rgba(37,99,235,0.35)] sm:w-auto"
              >
                <GithubMark className="size-4" />
                Fork on GitHub
                <ExternalLink className="size-3.5 opacity-70" aria-hidden />
              </a>
              <Link
                href="/"
                className="inline-flex w-full items-center justify-center gap-1.5 rounded-lg border border-[var(--border-subtle)] px-5 py-2.5 text-sm font-medium text-slate-600 transition-colors hover:bg-slate-50 sm:w-auto"
              >
                Back to dashboard
              </Link>
            </div>
            <p className="mt-6 text-xs text-slate-400">Use and modify freely for personal job tracking.</p>
          </div>
        </Reveal>
      </section>
    </div>
  );
}
