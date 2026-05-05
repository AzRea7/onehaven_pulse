type LoadingStateProps = {
  title?: string;
  message?: string;
};

export function LoadingState({
  title = "Loading",
  message = "Fetching the latest OneHaven data.",
}: LoadingStateProps) {
  return (
    <div className="rounded-2xl border border-slate-800 bg-slate-900/70 p-6">
      <div className="flex items-center gap-4">
        <div className="h-4 w-4 animate-spin rounded-full border-2 border-slate-500 border-t-white" />
        <div>
          <p className="font-medium text-white">{title}</p>
          <p className="mt-1 text-sm text-slate-400">{message}</p>
        </div>
      </div>
    </div>
  );
}
