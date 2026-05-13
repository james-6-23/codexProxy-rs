import type { AccountRow } from '../../../types'
import { formatResetAt, formatCountdown, usageBarColor } from '../formatters'

interface UsageBarProps {
  label: string
  pct: number
  resetAt?: string
}

function UsageBar({ label, pct, resetAt }: UsageBarProps) {
  const resetText = formatResetAt(resetAt)
  const countdown = formatCountdown(resetAt)
  return (
    <div>
      <div className="flex items-center gap-1.5">
        <span className="text-[11px] font-medium text-muted-foreground w-5 shrink-0">{label}</span>
        <div className="flex-1 h-1.5 rounded-full bg-muted overflow-hidden min-w-[72px]">
          <div className={`h-full rounded-full transition-all ${usageBarColor(pct)}`} style={{ width: `${Math.min(100, pct)}%` }} />
        </div>
        <span className="text-[12px] font-semibold w-[42px] text-right shrink-0">{pct.toFixed(1)}%</span>
      </div>
      {resetText && (
        <div className="text-[11px] font-medium text-muted-foreground mt-0.5 pl-[26px]">
          ⏱ {resetText}{countdown && <span className="ml-1 text-amber-600 dark:text-amber-400">({countdown})</span>}
        </div>
      )}
    </div>
  )
}

interface UsageCellProps {
  account: AccountRow
}

export function UsageCell({ account }: UsageCellProps) {
  const plan = (account.plan_type || '').toLowerCase()
  const has7d = account.usage_percent_7d !== null && account.usage_percent_7d !== undefined
  const has5h = account.usage_percent_5h !== null && account.usage_percent_5h !== undefined
  const isLimited = account.status === 'rate_limited'

  if (plan === 'free') {
    if (!has7d) return <span className="text-[12px] text-muted-foreground">-</span>
    return (
      <div className="w-40">
        <UsageBar label="7d" pct={account.usage_percent_7d!} resetAt={isLimited ? account.reset_7d_at : undefined} />
      </div>
    )
  }

  if (plan === 'pro' || plan === 'team') {
    if (!has5h && !has7d) return <span className="text-[12px] text-muted-foreground">-</span>
    return (
      <div className="w-48 space-y-1.5">
        {has5h && <UsageBar label="5h" pct={account.usage_percent_5h!} resetAt={isLimited ? account.reset_5h_at : undefined} />}
        {has7d && <UsageBar label="7d" pct={account.usage_percent_7d!} resetAt={isLimited ? account.reset_7d_at : undefined} />}
      </div>
    )
  }

  if (has7d) {
    return (
      <div className="w-40">
        <UsageBar label="7d" pct={account.usage_percent_7d!} resetAt={isLimited ? account.reset_7d_at : undefined} />
      </div>
    )
  }
  return <span className="text-[13px] text-muted-foreground">-</span>
}
