export type SourceStatus = "healthy" | "warning" | "failed";
export type ScanStatus = "success" | "partial" | "failed";

export interface OverviewData {
  last_scan: string;
  scan_status: ScanStatus;
  jobs_fetched_last_scan: number;
  jobs_matched_last_scan: number;
  new_jobs_last_scan: number;
  jobs_today: number;
  jobs_last_7_days: number;
  total_sources: number;
  healthy_sources: number;
  failed_sources: number;
}

export interface JobRecord {
  key: string;
  title: string;
  company: string;
  location: string;
  department: string;
  source_name: string;
  source_type: string;
  url: string;
  external_id: string;
  matched_keywords: string[];
  first_seen: string;
  last_seen: string;
  is_new: boolean;
}

export interface SourceRecord {
  name: string;
  type: string;
  status: SourceStatus;
  jobs_fetched: number;
  jobs_matched: number;
  last_scan: string;
  duration_ms: number;
  error: string | null;
}

export interface ActivitySourceResult {
  name: string;
  status: SourceStatus;
  jobs_fetched: number;
  jobs_matched: number;
  error: string | null;
}

export interface ActivityRun {
  timestamp: string;
  status: ScanStatus;
  duration_seconds: number;
  sources_total: number;
  sources_succeeded: number;
  sources_failed: number;
  jobs_fetched: number;
  jobs_matched: number;
  new_jobs: number;
  alerts_sent: number;
  sources?: ActivitySourceResult[];
}

export interface ConfigFilters {
  title_keywords_any?: string[];
  locations_any?: string[];
  excluded_keywords_any?: string[];
}

export interface ConfigSource {
  name: string;
  type: string;
  [key: string]: unknown;
}

export interface ConfigFile {
  filters: ConfigFilters;
  sources: ConfigSource[];
}

export type FetchKind = "config" | "network" | "not_found" | "parse" | "empty";

export type FetchResult<T> =
  | { ok: true; data: T }
  | { ok: false; error: string; kind: FetchKind };
