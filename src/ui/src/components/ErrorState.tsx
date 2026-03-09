export default function ErrorState({
  message,
  onRetry,
}: {
  message: string
  onRetry?: () => void
}) {
  return (
    <div className="flex items-center gap-3 bg-red-50 border border-red-200 rounded-lg px-4 py-3 text-sm">
      <span className="text-red-600 font-medium">Error:</span>
      <span className="text-red-700">{message}</span>
      {onRetry && (
        <button
          onClick={onRetry}
          className="ml-auto text-red-600 hover:text-red-800 font-medium underline"
        >
          Retry
        </button>
      )}
    </div>
  )
}
