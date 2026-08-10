"use client";

import { useEffect, useRef, useState } from "react";
import { useRouter } from "next/navigation";
import { Loader2, RotateCw, CheckCircle2, AlertTriangle } from "lucide-react";
import { Button } from "./ui/Button";
import { cn } from "@/lib/utils";

type ScanState = "idle" | "triggering" | "running" | "done" | "error";

const POLL_INTERVAL_MS = 4000;
const MAX_POLL_MS = 5 * 60 * 1000;

export function ScanNowButton() {
  const router = useRouter();
  const [state, setState] = useState<ScanState>("idle");
  const [message, setMessage] = useState<string | null>(null);
  const pollTimer = useRef<ReturnType<typeof setInterval> | null>(null);
  const pollDeadline = useRef(0);

  useEffect(() => {
    return () => {
      if (pollTimer.current) clearInterval(pollTimer.current);
    };
  }, []);

  function stopPolling() {
    if (pollTimer.current) {
      clearInterval(pollTimer.current);
      pollTimer.current = null;
    }
  }

  function pollStatus(since: string) {
    pollDeadline.current = Date.now() + MAX_POLL_MS;
    pollTimer.current = setInterval(async () => {
      if (Date.now() > pollDeadline.current) {
        stopPolling();
        setState("error");
        setMessage("Taking longer than expected — check Activity for status.");
        return;
      }
      try {
        const res = await fetch(`/api/scan/status?since=${encodeURIComponent(since)}`, { cache: "no-store" });
        const data = await res.json();
        if (!data.ok) {
          stopPolling();
          setState("error");
          setMessage(data.error ?? "Could not check scan status.");
          return;
        }
        if (data.state === "completed") {
          stopPolling();
          if (data.conclusion === "failure" || data.conclusion === "cancelled") {
            setState("error");
            setMessage(`Scan finished with status: ${data.conclusion}.`);
            return;
          }
          setState("done");
          setMessage("Scan complete. Dashboard will be updated shortly.");
          // Defer the refresh until after the bubble has had time to show —
          // router.refresh() re-renders the server tree this component lives
          // in, which can remount it and wipe the state we just set.
          setTimeout(() => {
            setState("idle");
            setMessage(null);
            router.refresh();
          }, 7000);
        }
      } catch {
        // Transient network hiccup while polling — keep trying until the deadline.
      }
    }, POLL_INTERVAL_MS);
  }

  async function handleClick() {
    setState("triggering");
    setMessage(null);
    try {
      const res = await fetch("/api/scan", { method: "POST" });
      const data = await res.json();
      if (!data.ok) {
        setState("error");
        setMessage(data.error ?? "Could not start scan.");
        return;
      }
      setState("running");
      pollStatus(data.triggeredAt);
    } catch {
      setState("error");
      setMessage("Could not reach the scan endpoint.");
    }
  }

  const isBusy = state === "triggering" || state === "running";
  const isError = state === "error";

  return (
    <div className="relative inline-flex">
      <Button variant="primary" size="sm" onClick={handleClick} disabled={isBusy} aria-busy={isBusy}>
        {isBusy ? (
          <Loader2 className="size-3.5 animate-spin" aria-hidden />
        ) : state === "done" ? (
          <CheckCircle2 className="size-3.5" aria-hidden />
        ) : state === "error" ? (
          <AlertTriangle className="size-3.5" aria-hidden />
        ) : (
          <RotateCw className="size-3.5" aria-hidden />
        )}
        {state === "triggering" ? "Starting…" : state === "running" ? "Scanning…" : state === "done" ? "Done" : "Scan Now"}
      </Button>

      {message ? (
        <div className="absolute right-0 top-full z-20 mt-2 w-max max-w-[min(260px,calc(100vw-2rem))]" role="status">
          <div
            className={cn(
              "absolute -top-[5px] right-3 size-2.5 rotate-45 border-l border-t",
              isError ? "border-red-200 bg-[var(--failed-soft)]" : "border-[var(--border-subtle)] bg-white"
            )}
            aria-hidden
          />
          <div
            className={cn(
              "relative rounded-lg border px-3 py-2 text-xs shadow-[0_8px_20px_rgba(15,23,42,0.12)]",
              isError ? "border-red-200 bg-[var(--failed-soft)] text-[var(--failed)]" : "border-[var(--border-subtle)] bg-white text-slate-600"
            )}
          >
            {message}
          </div>
        </div>
      ) : null}
    </div>
  );
}
