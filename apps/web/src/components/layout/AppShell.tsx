import type { ReactNode } from "react";

import { Sidebar } from "./Sidebar";
import { TopNav } from "./TopNav";

type AppShellProps = {
  children: ReactNode;
};

export function AppShell({ children }: AppShellProps) {
  return (
    <div className="min-h-screen bg-slate-950 text-slate-100">
      <div className="flex min-h-screen">
        <Sidebar />

        <div className="min-w-0 flex-1">
          <TopNav />

          <main className="mx-auto w-full max-w-7xl px-5 py-8 lg:px-8">
            {children}
          </main>
        </div>
      </div>
    </div>
  );
}
