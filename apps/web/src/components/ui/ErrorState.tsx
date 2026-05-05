import { ApiClientError, getErrorMessage } from "@/lib/api";

type ErrorStateProps = {
  title?: string;
  error?: unknown;
  message?: string;
};

export function ErrorState({
  title = "Something went wrong",
  error,
  message,
}: ErrorStateProps) {
  const resolvedMessage = message ?? getErrorMessage(error);

  const requestId =
    error instanceof ApiClientError && error.requestId
      ? error.requestId
      : null;

  return (
    <div className="rounded-2xl border border-red-900/60 bg-red-950/30 p-6">
      <p className="font-semibold text-red-100">{title}</p>
      <p className="mt-2 text-sm leading-6 text-red-200">{resolvedMessage}</p>

      {requestId ? (
        <p className="mt-3 font-mono text-xs text-red-300">
          request_id: {requestId}
        </p>
      ) : null}
    </div>
  );
}
