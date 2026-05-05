import Link from "next/link";

import { RelatedGeographiesPanel } from "@/components/geographies/RelatedGeographiesPanel";
import { getGeographyRelated } from "@/lib/api";

type GeographyPageProps = {
  params: Promise<{
    geo_id: string;
  }>;
};

export default async function GeographyPage({ params }: GeographyPageProps) {
  const { geo_id } = await params;

  let related = null;
  let error: string | null = null;

  try {
    related = await getGeographyRelated(geo_id);
  } catch {
    error = "Related geographies are unavailable.";
  }

  return (
    <div className="space-y-6 p-6">
      <div>
        <Link href="/geographies" className="text-sm text-slate-600 underline">
          Search geographies
        </Link>
        <p className="mt-4 text-sm text-slate-500">Geography</p>
        <h1 className="text-2xl font-semibold text-slate-950">{geo_id}</h1>
      </div>

      <RelatedGeographiesPanel data={related} error={error} />
    </div>
  );
}
