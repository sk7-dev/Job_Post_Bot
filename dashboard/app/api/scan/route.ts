import { NextResponse } from "next/server";

export const dynamic = "force-dynamic";

const WORKFLOW_FILE = "watch-jobs.yml";

function getRepoConfig() {
  return {
    owner: process.env.GITHUB_OWNER,
    repo: process.env.GITHUB_REPO,
    branch: process.env.GITHUB_BRANCH || "main",
    token: process.env.GITHUB_TOKEN,
  };
}

export async function POST() {
  const { owner, repo, branch, token } = getRepoConfig();

  if (!owner || !repo) {
    return NextResponse.json(
      { ok: false, error: "GITHUB_OWNER and GITHUB_REPO environment variables are not set." },
      { status: 500 }
    );
  }
  if (!token) {
    return NextResponse.json(
      { ok: false, error: "GITHUB_TOKEN is not set. A token with Actions write access is required to trigger a scan." },
      { status: 500 }
    );
  }

  const headers = {
    Authorization: `Bearer ${token}`,
    Accept: "application/vnd.github+json",
    "X-GitHub-Api-Version": "2022-11-28",
  };

  // Avoid dispatching a second run on top of one that's already in flight
  // (whether started by this button or by the scheduled cron trigger).
  try {
    const runsRes = await fetch(
      `https://api.github.com/repos/${owner}/${repo}/actions/workflows/${WORKFLOW_FILE}/runs?per_page=1`,
      { headers, cache: "no-store" }
    );
    if (runsRes.ok) {
      const runsData = await runsRes.json();
      const latest = runsData.workflow_runs?.[0];
      if (latest && (latest.status === "in_progress" || latest.status === "queued")) {
        return NextResponse.json({ ok: false, error: "A scan is already running." }, { status: 409 });
      }
    }
  } catch {
    // Non-fatal — fall through and attempt the dispatch anyway.
  }

  const triggeredAt = new Date().toISOString();

  let dispatchRes: Response;
  try {
    dispatchRes = await fetch(
      `https://api.github.com/repos/${owner}/${repo}/actions/workflows/${WORKFLOW_FILE}/dispatches`,
      {
        method: "POST",
        headers: { ...headers, "Content-Type": "application/json" },
        body: JSON.stringify({ ref: branch }),
      }
    );
  } catch {
    return NextResponse.json({ ok: false, error: "Could not reach GitHub." }, { status: 502 });
  }

  if (dispatchRes.status === 204) {
    return NextResponse.json({ ok: true, triggeredAt });
  }

  if (dispatchRes.status === 401 || dispatchRes.status === 403) {
    return NextResponse.json(
      {
        ok: false,
        error:
          "GitHub rejected the request. The configured GITHUB_TOKEN needs 'Actions: write' permission (fine-grained) or the 'repo' scope (classic) to trigger a workflow.",
      },
      { status: dispatchRes.status }
    );
  }

  if (dispatchRes.status === 404) {
    return NextResponse.json(
      { ok: false, error: `Workflow "${WORKFLOW_FILE}" was not found on branch "${branch}".` },
      { status: 404 }
    );
  }

  const text = await dispatchRes.text().catch(() => "");
  return NextResponse.json(
    { ok: false, error: `GitHub returned an unexpected status (${dispatchRes.status}). ${text}`.trim() },
    { status: dispatchRes.status }
  );
}
