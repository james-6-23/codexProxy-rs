interface CompactStatProps {
  label: string
  chipLabel?: string
  value: number
  tone: 'neutral' | 'success' | 'warning' | 'danger'
}

export function CompactStat({ label, chipLabel, value, tone }: CompactStatProps) {
  const toneStyle = {
    neutral: {
      chip: 'bg-slate-500/10 text-slate-600 dark:bg-slate-500/20 dark:text-slate-300',
      dot: 'bg-slate-500',
    },
    success: {
      chip: 'bg-emerald-500/10 text-emerald-600 dark:bg-emerald-500/20 dark:text-emerald-300',
      dot: 'bg-emerald-500',
    },
    warning: {
      chip: 'bg-amber-500/10 text-amber-600 dark:bg-amber-500/20 dark:text-amber-300',
      dot: 'bg-amber-500',
    },
    danger: {
      chip: 'bg-red-500/10 text-red-600 dark:bg-red-500/20 dark:text-red-300',
      dot: 'bg-red-500',
    },
  }[tone]

  return (
    <div className="flex items-center justify-between rounded-2xl border border-border bg-white/65 px-4 py-3 shadow-[inset_0_1px_0_rgba(255,255,255,0.7)]">
      <div className="min-w-0">
        <div className="text-[12px] font-semibold text-muted-foreground">{label}</div>
        <div className="mt-1 text-[24px] font-bold leading-none tracking-tight text-foreground">{value}</div>
      </div>
      <div className={`inline-flex items-center gap-1.5 rounded-full px-2.5 py-1 text-[12px] font-semibold ${toneStyle.chip}`}>
        <span className={`size-2 rounded-full ${toneStyle.dot}`} />
        {chipLabel ?? label}
      </div>
    </div>
  )
}

interface SchedulerChipProps {
  label: string
  value: number
  tone: 'neutral' | 'success' | 'warning' | 'danger'
}

export function SchedulerChip({ label, value, tone }: SchedulerChipProps) {
  const toneStyle = {
    neutral: 'bg-slate-500/10 text-slate-600 dark:bg-slate-500/20 dark:text-slate-300',
    success: 'bg-emerald-500/10 text-emerald-600 dark:bg-emerald-500/20 dark:text-emerald-300',
    warning: 'bg-amber-500/10 text-amber-600 dark:bg-amber-500/20 dark:text-amber-300',
    danger: 'bg-red-500/10 text-red-600 dark:bg-red-500/20 dark:text-red-300',
  }[tone]

  return (
    <span className={`inline-flex items-center gap-1.5 rounded-full px-2.5 py-1 font-semibold ${toneStyle}`}>
      <span>{label}</span>
      <span>{value}</span>
    </span>
  )
}
