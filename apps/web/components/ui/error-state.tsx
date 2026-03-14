type ErrorStateProps = {
  title?: string;
  message: string;
};

export function ErrorState({ title = "Something went wrong", message }: ErrorStateProps) {
  return (
    <div className="rounded-3xl border border-rose-500/20 bg-rose-500/[0.08] px-5 py-6">
      <h3 className="text-base font-semibold text-rose-100">{title}</h3>
      <p className="mt-2 text-sm text-rose-200/80">{message}</p>
    </div>
  );
}
