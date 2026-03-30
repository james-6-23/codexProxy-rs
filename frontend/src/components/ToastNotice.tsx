import { AlertCircle, CheckCircle2 } from 'lucide-react'
import type { ToastState } from '../types'

export default function ToastNotice({ toast }: { toast: ToastState | null }) {
  if (!toast) return null

  const isError = toast.type === 'error'
  const toneClassName = isError
    ? 'border-red-500/20 bg-red-500/10 text-red-700 shadow-[0_12px_28px_rgba(239,68,68,0.12)] dark:border-red-500/20 dark:bg-red-500/12 dark:text-red-200'
    : 'border-emerald-500/20 bg-emerald-500/10 text-emerald-700 shadow-[0_12px_28px_rgba(16,185,129,0.12)] dark:border-emerald-500/20 dark:bg-emerald-500/12 dark:text-emerald-200'

  return (
    <div
      className={`pointer-events-none fixed top-4 right-4 z-[2000] flex max-w-[min(320px,calc(100vw-1.5rem))] items-start gap-2.5 rounded-xl border px-3 py-2.5 text-[13px] font-medium backdrop-blur-xl max-sm:top-3 max-sm:right-3 ${toneClassName}`}
      style={{ animation: 'toast-slide-in 0.22s ease' }}
      role="status"
      aria-live="polite"
    >
      <span className="mt-0.5 shrink-0 opacity-90">
        {isError ? <AlertCircle className="size-4" /> : <CheckCircle2 className="size-4" />}
      </span>
      <span className="min-w-0 break-words leading-5">{toast.msg}</span>
    </div>
  )
}
