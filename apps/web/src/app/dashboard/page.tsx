import { AppShell } from "@/components/layout/AppShell";

export default function DashboardPage() {
  return (
    <AppShell>
      <div className="space-y-3">
        <h1 className="text-3xl font-bold text-white">Dashboard</h1>
        <p className="text-slate-300">
          National market-cycle overview will be built here.
        </p>
      </div>
    </AppShell>
  );
}
