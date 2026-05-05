import { LoadingState } from "@/components/ui/LoadingState";

export default function Loading() {
  return (
    <div className="min-h-screen bg-slate-950 p-8 text-slate-100">
      <LoadingState />
    </div>
  );
}
