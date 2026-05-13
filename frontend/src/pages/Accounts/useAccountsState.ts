import { useState, useCallback } from 'react'
import type { AccountRow } from '../../types'

export function useAccountsState() {
  const [page, setPage] = useState(1)
  const [statusFilter, setStatusFilter] = useState<'all' | 'normal' | 'rate_limited' | 'banned'>('all')
  const [searchQuery, setSearchQuery] = useState('')
  const [planFilter, setPlanFilter] = useState<'all' | 'pro' | 'team' | 'free'>('all')
  const [sortKey, setSortKey] = useState<'requests' | 'usage' | 'importTime' | null>(null)
  const [sortDir, setSortDir] = useState<'asc' | 'desc'>('desc')
  const [selected, setSelected] = useState<Set<number>>(new Set())
  const [refreshingIds, setRefreshingIds] = useState<Set<number>>(new Set())
  const [testingAccount, setTestingAccount] = useState<AccountRow | null>(null)
  const [usageAccount, setUsageAccount] = useState<AccountRow | null>(null)

  const toggleSelect = useCallback((id: number) => {
    setSelected((prev) => {
      const next = new Set(prev)
      if (next.has(id)) next.delete(id)
      else next.add(id)
      return next
    })
  }, [])

  const clearSelection = useCallback(() => {
    setSelected(new Set())
  }, [])

  const addRefreshingId = useCallback((id: number) => {
    setRefreshingIds((prev) => new Set(prev).add(id))
  }, [])

  const removeRefreshingId = useCallback((id: number) => {
    setRefreshingIds((prev) => {
      const next = new Set(prev)
      next.delete(id)
      return next
    })
  }, [])

  return {
    page,
    setPage,
    statusFilter,
    setStatusFilter,
    searchQuery,
    setSearchQuery,
    planFilter,
    setPlanFilter,
    sortKey,
    setSortKey,
    sortDir,
    setSortDir,
    selected,
    setSelected,
    toggleSelect,
    clearSelection,
    refreshingIds,
    addRefreshingId,
    removeRefreshingId,
    testingAccount,
    setTestingAccount,
    usageAccount,
    setUsageAccount,
  }
}
