import type { ReactNode } from "react";

import { AppNavigation } from "@/components/layout/AppNavigation";
import { theme } from "@/lib/theme";

type AppShellProps = {
  children: ReactNode;
};

export function AppShell({ children }: AppShellProps) {
  return (
    <div className={theme.shell}>
      <AppNavigation />
      <main className={theme.page}>{children}</main>
    </div>
  );
}
