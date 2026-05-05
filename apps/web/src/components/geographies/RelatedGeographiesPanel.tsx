"use client";

import Link from "next/link";

import type {
  GeographyRelatedResponse,
  GeographyRelationshipItem,
} from "@/lib/api";

type RelatedGeographiesPanelProps = {
  data: GeographyRelatedResponse | null;
  isLoading?: boolean;
  error?: string | null;
};

function geoHref(geoId: string): string {
  return `/geographies/${encodeURIComponent(geoId)}`;
}

function relationshipLabel(item: GeographyRelationshipItem, direction: "parent" | "child"): string {
  const geography = direction === "parent" ? item.parent : item.child;
  return geography.display_name || geography.name || geography.geo_id;
}

function GeographyList({
  title,
  items,
  direction,
}: {
  title: string;
  items: GeographyRelationshipItem[];
  direction: "parent" | "child";
}) {
  return (
    <section className="rounded-2xl border border-slate-200 bg-white p-4 shadow-sm">
      <div className="mb-3 flex items-center justify-between gap-3">
        <h3 className="text-sm font-semibold text-slate-900">{title}</h3>
        <span className="rounded-full bg-slate-100 px-2 py-1 text-xs text-slate-600">
          {items.length}
        </span>
      </div>

      {items.length === 0 ? (
        <p className="text-sm text-slate-500">No related geographies found.</p>
      ) : (
        <div className="space-y-2">
          {items.map((item) => {
            const geography = direction === "parent" ? item.parent : item.child;

            return (
              <Link
                key={`${item.parent.geo_id}-${item.child.geo_id}-${item.relationship_type}-${item.source}`}
                href={geoHref(geography.geo_id)}
                className="block rounded-xl border border-slate-100 p-3 transition hover:border-slate-300 hover:bg-slate-50"
              >
                <div className="flex items-start justify-between gap-3">
                  <div>
                    <div className="text-sm font-medium text-slate-900">
                      {relationshipLabel(item, direction)}
                    </div>
                    <div className="mt-1 text-xs text-slate-500">
                      {geography.geo_type} · {geography.geo_id}
                    </div>
                  </div>

                  <div className="text-right text-xs text-slate-500">
                    <div>{item.relationship_type}</div>
                    <div>{Number(item.confidence_score).toFixed(2)}</div>
                  </div>
                </div>
              </Link>
            );
          })}
        </div>
      )}
    </section>
  );
}

export function RelatedGeographiesPanel({
  data,
  isLoading = false,
  error = null,
}: RelatedGeographiesPanelProps) {
  if (isLoading) {
    return (
      <section className="rounded-2xl border border-slate-200 bg-white p-4 shadow-sm">
        <h2 className="text-base font-semibold text-slate-900">Related geographies</h2>
        <p className="mt-2 text-sm text-slate-500">Loading related geographies…</p>
      </section>
    );
  }

  if (error) {
    return (
      <section className="rounded-2xl border border-red-200 bg-white p-4 shadow-sm">
        <h2 className="text-base font-semibold text-slate-900">Related geographies</h2>
        <p className="mt-2 text-sm text-red-600">{error}</p>
      </section>
    );
  }

  if (!data) {
    return (
      <section className="rounded-2xl border border-slate-200 bg-white p-4 shadow-sm">
        <h2 className="text-base font-semibold text-slate-900">Related geographies</h2>
        <p className="mt-2 text-sm text-slate-500">No relationship data is available.</p>
      </section>
    );
  }

  return (
    <section className="space-y-4">
      <div>
        <h2 className="text-base font-semibold text-slate-900">Related geographies</h2>
        <p className="mt-1 text-sm text-slate-500">
          Parent and child jurisdictions from the canonical geography relationship graph.
        </p>
      </div>

      <div className="grid gap-4 lg:grid-cols-2">
        <GeographyList title="Parents" items={data.parents} direction="parent" />
        <GeographyList title="Children" items={data.children} direction="child" />
      </div>
    </section>
  );
}
