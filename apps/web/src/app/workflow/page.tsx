import { AppShell } from "@/components/layout/AppShell";
import { WorkflowCard } from "@/components/workflow/WorkflowCard";

const WORKFLOW = [
  {
    step: "Step 1",
    title: "Screen markets",
    description:
      "Start broad. Use investor presets to find markets that fit your thesis before looking at individual properties.",
    href: "/screener",
    cta: "Open screener",
    bullets: [
      "Use Watchlist to find promising but imperfect markets.",
      "Use Missing Data Review to find markets that need manual verification.",
      "Use Rent Momentum and Affordable Watchlist to narrow your first pass.",
    ],
  },
  {
    step: "Step 2",
    title: "Read the market signal",
    description:
      "Open a market page to see stance, score, drivers, risks, missing inputs, and rule trace.",
    href: "/markets/metro_19820",
    cta: "View Detroit example",
    bullets: [
      "Do not treat Watchlist as buy.",
      "Use drivers and risks as your research checklist.",
      "Check material missing inputs before trusting the stance.",
    ],
  },
  {
    step: "Step 3",
    title: "Compare candidate markets",
    description:
      "Compare markets side by side before spending time on property-level underwriting.",
    href: "/compare",
    cta: "Compare markets",
    bullets: [
      "Compare affordability, rent growth, labor, and price momentum.",
      "Use comparison to prioritize research time.",
      "Avoid markets where the signal is weak or stale.",
    ],
  },
  {
    step: "Step 4",
    title: "Check spatial and data quality context",
    description:
      "Use map and source freshness to understand geography and whether the underlying data is current enough.",
    href: "/map",
    cta: "Open map",
    bullets: [
      "Use the map to spot regional patterns.",
      "Use Data Health when a signal looks suspicious.",
      "Do not rely on stale or incomplete source inputs.",
    ],
  },
];

export default function WorkflowPage() {
  return (
    <AppShell>
      <section className="space-y-8">
        <div className="rounded-3xl border border-slate-800 bg-gradient-to-br from-slate-900 to-slate-950 p-6 shadow-sm">
          <p className="text-sm font-medium uppercase tracking-[0.24em] text-cyan-300">
            Investor Workflow
          </p>
          <h1 className="mt-4 max-w-4xl text-3xl font-bold tracking-tight text-white md:text-5xl">
            Use OneStream Pulse to decide where to focus your real estate research.
          </h1>
          <p className="mt-4 max-w-3xl text-base leading-7 text-slate-400">
            OneStream Pulse is a market-selection system. It helps you identify,
            compare, and monitor areas before you underwrite individual deals.
            It does not replace property-level underwriting.
          </p>
        </div>

        <div className="grid gap-5 lg:grid-cols-2">
          {WORKFLOW.map((item) => (
            <WorkflowCard key={item.step} {...item} />
          ))}
        </div>

        <div className="rounded-2xl border border-amber-700/50 bg-amber-950/30 p-5">
          <h2 className="text-lg font-semibold text-amber-200">
            Investor rule of thumb
          </h2>
          <p className="mt-2 text-sm leading-6 text-amber-100/80">
            Attractive means high-priority research. Watchlist means promising
            but imperfect. Neither means buy. A purchase decision still depends
            on property price, rent, taxes, insurance, repairs, financing, and
            block-level risk.
          </p>
        </div>
      </section>
    </AppShell>
  );
}
