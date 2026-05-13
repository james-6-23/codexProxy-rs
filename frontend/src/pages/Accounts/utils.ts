import type { AccountRow } from '../../types'

export function filterAccounts(
  accounts: AccountRow[],
  statusFilter: 'all' | 'normal' | 'rate_limited' | 'banned',
  planFilter: 'all' | 'pro' | 'team' | 'free',
  searchQuery: string
): AccountRow[] {
  return accounts.filter((account) => {
    // 状态过滤
    switch (statusFilter) {
      case 'normal':
        if (account.status !== 'active' && account.status !== 'ready') return false
        break
      case 'rate_limited':
        if (account.status !== 'rate_limited') return false
        break
      case 'banned':
        if (account.status !== 'unauthorized') return false
        break
    }
    // 套餐过滤
    if (planFilter !== 'all') {
      const plan = (account.plan_type || '').toLowerCase()
      if (plan !== planFilter) return false
    }
    // 搜索过滤
    if (searchQuery) {
      const q = searchQuery.toLowerCase()
      const email = (account.email || '').toLowerCase()
      const name = (account.name || '').toLowerCase()
      if (!email.includes(q) && !name.includes(q)) return false
    }
    return true
  })
}

export function sortAccounts(
  accounts: AccountRow[],
  sortKey: 'requests' | 'usage' | 'importTime' | null,
  sortDir: 'asc' | 'desc'
): AccountRow[] {
  if (!sortKey) return accounts

  return [...accounts].sort((a, b) => {
    let diff = 0
    if (sortKey === 'requests') {
      diff = ((a.success_requests ?? 0) + (a.error_requests ?? 0)) - ((b.success_requests ?? 0) + (b.error_requests ?? 0))
    } else if (sortKey === 'usage') {
      diff = (a.usage_percent_7d ?? -1) - (b.usage_percent_7d ?? -1)
    } else if (sortKey === 'importTime') {
      diff = new Date(a.created_at || 0).getTime() - new Date(b.created_at || 0).getTime()
    }
    return sortDir === 'asc' ? diff : -diff
  })
}

export function calculateAccountStats(accounts: AccountRow[]) {
  return {
    total: accounts.length,
    normal: accounts.filter((a) => a.status === 'active' || a.status === 'ready').length,
    rateLimited: accounts.filter((a) => a.status === 'rate_limited').length,
    banned: accounts.filter((a) => a.status === 'unauthorized').length,
    healthy: accounts.filter((a) => a.health_tier === 'healthy').length,
    warm: accounts.filter((a) => a.health_tier === 'warm').length,
    risky: accounts.filter((a) => a.health_tier === 'risky').length,
  }
}
