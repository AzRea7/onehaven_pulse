import Link from "next/link";

import { EmptyState } from "@/components/ui/EmptyState";

export default function NotFound() {
  return (
    <div className="min-h-screen bg-slate-950 p-8 text-slate-100">
      <div className="mx-auto max-w-3xl space-y-6">
        <EmptyState
          title="Page not found"
          message="The page or market you requested does not exist."
        />

        <Link
          href="/dashboard"
          className="inline-flex rounded-xl bg-white px-4 py-2 text-sm font-medium text-slate-950 transition hover:bg-slate-200"
        >
          Back to dashboard
        </Link>
      </div>
    </div>
  );
}
