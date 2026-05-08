export const theme = {
  shell:
    "min-h-screen bg-[#050608] text-[#F4F7FA] selection:bg-[#7CFFB2]/30 selection:text-white",
  page:
    "mx-auto w-full max-w-7xl px-4 py-8 lg:px-6",
  nav:
    "sticky top-0 z-40 border-b border-white/10 bg-[#050608]/90 backdrop-blur-xl",
  navInner:
    "mx-auto flex max-w-7xl flex-col gap-3 px-4 py-3 lg:flex-row lg:items-center lg:justify-between lg:px-6",
  card:
    "rounded-3xl border border-white/10 bg-white/[0.045] shadow-[0_24px_80px_rgba(0,0,0,0.35)] backdrop-blur",
  cardTight:
    "rounded-2xl border border-white/10 bg-white/[0.045] shadow-[0_18px_60px_rgba(0,0,0,0.28)] backdrop-blur",
  panel:
    "rounded-2xl border border-white/10 bg-[#0A0D12]/90",
  hero:
    "rounded-[2rem] border border-white/10 bg-[radial-gradient(circle_at_20%_10%,rgba(124,255,178,0.18),transparent_30%),radial-gradient(circle_at_80%_0%,rgba(88,166,255,0.16),transparent_32%),linear-gradient(135deg,rgba(255,255,255,0.08),rgba(255,255,255,0.025))] shadow-[0_32px_120px_rgba(0,0,0,0.45)]",
  eyebrow:
    "text-sm font-semibold uppercase tracking-[0.24em] text-[#7CFFB2]",
  h1:
    "text-4xl font-semibold tracking-[-0.04em] text-white md:text-6xl",
  h2:
    "text-2xl font-semibold tracking-[-0.03em] text-white",
  h3:
    "text-lg font-semibold tracking-[-0.02em] text-white",
  muted:
    "text-[#9AA7B7]",
  body:
    "text-sm leading-6 text-[#AEB8C6]",
  primaryButton:
    "inline-flex items-center justify-center rounded-2xl bg-[#7CFFB2] px-5 py-3 text-sm font-semibold text-[#06110A] shadow-[0_0_40px_rgba(124,255,178,0.25)] transition hover:bg-[#A7FFC9]",
  secondaryButton:
    "inline-flex items-center justify-center rounded-2xl border border-white/12 bg-white/[0.04] px-5 py-3 text-sm font-semibold text-white transition hover:border-white/25 hover:bg-white/[0.08]",
  chip:
    "rounded-full border border-white/10 bg-white/[0.055] px-3 py-1 text-xs font-semibold text-[#DDE7F3]",
  activeChip:
    "rounded-full border border-[#7CFFB2] bg-[#7CFFB2] px-3 py-1 text-xs font-semibold text-[#06110A]",
  input:
    "rounded-xl border border-white/10 bg-[#070A0F] px-3 py-2 text-sm text-white outline-none transition placeholder:text-[#647084] focus:border-[#7CFFB2]",
  select:
    "rounded-xl border border-white/10 bg-[#070A0F] px-3 py-2 text-sm text-white outline-none transition focus:border-[#7CFFB2]",
};

export function cx(...classes: Array<string | false | null | undefined>): string {
  return classes.filter(Boolean).join(" ");
}
