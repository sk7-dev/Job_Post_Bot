import "server-only";

import type {
  ActivityRun,
  ConfigFile,
  FetchKind,
  FetchResult,
  JobRecord,
  OverviewData,
  SourceRecord,
} from "./types";

const DEFAULT_REVALIDATE_SECONDS = 45;

function getRepoConfig() {
  return {
    owner: process.env.GITHUB_OWNER,
    repo: process.env.GITHUB_REPO,
    branch: process.env.GITHUB_BRANCH || "main",
    token: process.env.GITHUB_TOKEN,
  };
}

function err<T>(kind: FetchKind, error: string): FetchResult<T> {
  return { ok: false, kind, error };
}

/**
 * Fetches a JSON file from the repo's raw GitHub content at runtime (not
 * bundled at build time), so dashboard_data/*.json updates show up without
 * a Netlify redeploy. Never throws — every failure mode becomes a typed
 * result so pages can render a friendly state instead of crashing.
 */
async function fetchJsonFile<T>(
  path: string,
  revalidateSeconds: number = DEFAULT_REVALIDATE_SECONDS
): Promise<FetchResult<T>> {
  const { owner, repo, branch, token } = getRepoConfig();

  if (!owner || !repo) {
    return err(
      "config",
      "GITHUB_OWNER and GITHUB_REPO environment variables are not set."
    );
  }

  const url = `https://raw.githubusercontent.com/${owner}/${repo}/${branch}/${path}`;
  const headers: Record<string, string> = {};
  if (token) {
    headers.Authorization = `Bearer ${token}`;
  }

  let response: Response;
  try {
    response = await fetch(url, {
      headers,
      next: { revalidate: revalidateSeconds },
    });
  } catch {
    return err("network", "Could not reach GitHub. It may be temporarily unavailable.");
  }

  if (response.status === 404) {
    return err("not_found", `${path} was not found in ${owner}/${repo}@${branch}.`);
  }

  if (!response.ok) {
    return err("network", `GitHub returned an unexpected status (${response.status}).`);
  }

  const text = await response.text();
  if (!text || !text.trim()) {
    return err("empty", `${path} is empty.`);
  }

  let data: unknown;
  try {
    data = JSON.parse(text);
  } catch {
    return err("parse", `${path} does not contain valid JSON.`);
  }

  return { ok: true, data: data as T };
}

function asArray<T>(result: FetchResult<unknown>, path: string): FetchResult<T[]> {
  if (!result.ok) return result;
  if (!Array.isArray(result.data)) {
    return err("parse", `${path} was expected to be a JSON array.`);
  }
  return { ok: true, data: result.data as T[] };
}

function asObject<T>(result: FetchResult<unknown>, path: string): FetchResult<T> {
  if (!result.ok) return result;
  if (typeof result.data !== "object" || result.data === null || Array.isArray(result.data)) {
    return err("parse", `${path} was expected to be a JSON object.`);
  }
  return { ok: true, data: result.data as T };
}

export async function getOverview(): Promise<FetchResult<OverviewData>> {
  const path = "dashboard_data/overview.json";
  const result = await fetchJsonFile<unknown>(path);
  return asObject<OverviewData>(result, path);
}

export async function getJobs(): Promise<FetchResult<JobRecord[]>> {
  const path = "dashboard_data/jobs.json";
  const result = await fetchJsonFile<unknown>(path);
  return asArray<JobRecord>(result, path);
}

export async function getSources(): Promise<FetchResult<SourceRecord[]>> {
  const path = "dashboard_data/sources.json";
  const result = await fetchJsonFile<unknown>(path);
  return asArray<SourceRecord>(result, path);
}

export async function getActivity(): Promise<FetchResult<ActivityRun[]>> {
  const path = "dashboard_data/activity.json";
  const result = await fetchJsonFile<unknown>(path);
  return asArray<ActivityRun>(result, path);
}

export async function getConfig(): Promise<FetchResult<ConfigFile>> {
  const path = "config.json";
  const result = await fetchJsonFile<unknown>(path);
  return asObject<ConfigFile>(result, path);
}
