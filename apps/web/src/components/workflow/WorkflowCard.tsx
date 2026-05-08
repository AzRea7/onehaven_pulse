import Link from "next/link";

type WorkflowCardProps = {
  step: string;
  title: string;
  description: string;
  href: string;
  cta: string;
  bullets?: string[];
};

export function WorkflowCard({
  step,
  title,
  description,
  href,
  cta,
  bullets = [],
}: WorkflowCardProps) {
  return (
    <div className="rounded-2xl border border-slate-800 bg-slate-900/70 p-5 shadow-sm">
      <div className="text-xs font-semibold uppercase tracking-[0.22em] text-cyan-300">
        {step}
      </div>
      <h3 className="mt-3 text-xl font-semibold text-white">{title}</h3>
      <p className="mt-2 text-sm leading-6 text-slate-400">{description}</p>

      {bullets.length > 0 ? (
        <ul className="mt-4 space-y-2 text-sm text-slate-300">
          {bullets.map((bullet) => (
            <li key={bullet} className="flex gap-2">
              <span className="mt-2 h-1.5 w-1.5 rounded-full bg-cyan-300" />
              <span>{bullet}</span>
            </li>
          ))}
        </ul>
      ) : null}

      <Link
        href={href}
        className="mt-5 inline-flex rounded-xl bg-cyan-300 px-4 py-2 text-sm font-semibold text-slate-950 transition hover:bg-cyan-200"
      >
        {cta}
      </Link>
    </div>
  );
}
