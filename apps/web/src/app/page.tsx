import Link from "next/link";

import { AppShell } from "@/components/layout/AppShell";
import { theme } from "@/lib/theme";

const QUICK_ACTIONS = [
  {
    href: "/workflow",
    title: "Start workflow",
    description: "Follow the investor process from screen → signal → compare → verify.",
  },
  {
    href: "/screener",
    title: "Screen markets",
    description: "Use investor presets to find Watchlist, Mixed, and high-confidence markets.",
  },
  {
    href: "/markets/metro_19820",
    title: "Area readout",
    description: "Open a fast real estate investment summary for Detroit metro.",
  },
  {
    href: "/map",
    title: "Map view",
    description: "Filter markets spatially, including by state like Michigan.",
  },
];

export default function HomePage() {
  return (
    <AppShell>
      <section className="space-y-8">
        <div className={`${theme.hero} p-6 md:p-10`}>
          <p className={theme.eyebrow}>OneStream Pulse</p>
          <h1 className={`${theme.h1} mt-5 max-w-5xl`}>
            The market analysis tool that tells you where to focus.
          </h1>
          <p className="mt-5 max-w-3xl text-lg leading-8 text-[#AEB8C6]">
            Screen real estate markets, read deterministic investor signals,
            compare areas, expose missing data, and decide where to spend your
            underwriting time.
          </p>

          <div className="mt-8 flex flex-wrap gap-3">
            <Link href="/workflow" className={theme.primaryButton}>
              Start investor workflow
            </Link>
            <Link href="/screener" className={theme.secondaryButton}>
              Open screener
            </Link>
          </div>
        </div>

        <div className="grid gap-5 md:grid-cols-2 xl:grid-cols-4">
          {QUICK_ACTIONS.map((action) => (
            <Link
              key={action.href}
              href={action.href}
              className={`${theme.cardTight} p-5 transition hover:border-[#7CFFB2]/60 hover:bg-white/[0.07]`}
            >
              <h2 className={theme.h3}>{action.title}</h2>
              <p className={`mt-2 ${theme.body}`}>{action.description}</p>
            </Link>
          ))}
        </div>

        <div className={`${theme.card} p-6`}>
          <p className={theme.eyebrow}>Investor standard</p>
          <h2 className={`${theme.h2} mt-3`}>This is not a deal analyzer.</h2>
          <p className={`mt-3 max-w-4xl ${theme.body}`}>
            OneStream Pulse helps you decide which markets deserve research.
            It does not tell you to buy a specific property. Use it to narrow
            your market universe before underwriting taxes, insurance, rent,
            repairs, financing, and neighborhood-level risk.
          </p>
        </div>
      </section>
    </AppShell>
  );
}
