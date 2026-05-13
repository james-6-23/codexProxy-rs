export function formatHealthTier(healthTier?: string, t?: any) {
  if (!t) return 'Unknown'
  switch (healthTier) {
    case 'healthy':
      return t('accounts.healthy')
    case 'warm':
      return t('accounts.warm')
    case 'risky':
      return t('accounts.risky')
    case 'banned':
      return t('accounts.quarantine')
    default:
      return t('accounts.unknown')
  }
}

export function formatResetAt(resetAt: string | undefined): string | null {
  if (!resetAt) return null
  const d = new Date(resetAt)
  if (d.getTime() <= Date.now()) return null
  const pad = (n: number) => String(n).padStart(2, '0')
  return `${pad(d.getMonth() + 1)}-${pad(d.getDate())} ${pad(d.getHours())}:${pad(d.getMinutes())}`
}

export function formatCountdown(resetAt: string | undefined): string | null {
  if (!resetAt) return null
  const d = new Date(resetAt)
  const diff = d.getTime() - Date.now()
  if (diff <= 0) return null
  const totalMin = Math.floor(diff / 60000)
  const days = Math.floor(totalMin / 1440)
  const hours = Math.floor((totalMin % 1440) / 60)
  const mins = totalMin % 60
  if (days > 0) return `${days}天${hours}时${mins}分`
  if (hours > 0) return `${hours}时${mins}分`
  return `${mins}分`
}

export function usageBarColor(pct: number): string {
  if (pct >= 90) return 'bg-red-500'
  if (pct >= 70) return 'bg-amber-500'
  return 'bg-emerald-500'
}

export function downloadBlob(blob: Blob, filename: string) {
  const url = URL.createObjectURL(blob)
  const a = document.createElement('a')
  a.href = url
  a.download = filename
  document.body.appendChild(a)
  a.click()
  document.body.removeChild(a)
  URL.revokeObjectURL(url)
}

export function formatTestErrorMessage(message: string) {
  const normalized = message.trim()
  const jsonStart = normalized.indexOf('{')

  if (jsonStart === -1) {
    return normalized
  }

  const prefix = normalized.slice(0, jsonStart).trim().replace(/[：:]\s*$/, '')
  const jsonText = normalized.slice(jsonStart)

  try {
    const parsed = JSON.parse(jsonText)
    const prettyJson = JSON.stringify(parsed, null, 2)
    return prefix ? `${prefix}\n${prettyJson}` : prettyJson
  } catch {
    return normalized
  }
}
