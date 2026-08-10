import { NextRequest, NextResponse } from "next/server";

export const dynamic = "force-dynamic";

const WORKFLOW_FILE = "watch-jobs.yml";

interface WorkflowRun {
  status: string;
  conclusion: string | null;
  created_at: string;
  html_url: string;
}

export async function GET(request: NextRequest) {
  const since = request.nextUrl.searchParams.get("since");
  const owner = process.env.GITHUB_OWNER;
  const repo = process.env.GITHUB_REPO;
  const token = process.env.GITHUB_TOKEN;

  if (!owner || !repo || !token) {
    return NextResponse.json({ ok: false, error: "Dashboard is not configured." }, { status: 500 });
  }
  if (!since) {
    return NextResponse.json({ ok: false, error: "Missing 'since' parameter." }, { status: 400 });
  }

  const headers = {
    Authorization: `Bearer ${token}`,
    Accept: "application/vnd.github+json",
    "X-GitHub-Api-Version": "2022-11-28",
  };

  let res: Response;
  try {
    res = await fetch(
      `https://api.github.com/repos/${owner}/${repo}/actions/workflows/${WORKFLOW_FILE}/runs?event=workflow_dispatch&per_page=5`,
      { headers, cache: "no-store" }
    );
  } catch {
    return NextResponse.json({ ok: false, error: "Could not reach GitHub." }, { status: 502 });
  }

  if (!res.ok) {
    return NextResponse.json({ ok: false, error: `GitHub returned an unexpected status (${res.status}).` }, { status: res.status });
  }

  const data = await res.json();
  const runs: WorkflowRun[] = data.workflow_runs ?? [];
  const sinceMs = new Date(since).getTime() - 15_000; // small buffer for clock drift
  const run = runs.find((r) => new Date(r.created_at).getTime() >= sinceMs);

  if (!run) {
    return NextResponse.json({ ok: true, state: "pending" });
  }

  if (run.status !== "completed") {
    return NextResponse.json({ ok: true, state: "running", runUrl: run.html_url });
  }

  return NextResponse.json({ ok: true, state: "completed", conclusion: run.conclusion, runUrl: run.html_url });
}
