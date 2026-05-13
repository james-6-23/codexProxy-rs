import { useCallback } from 'react'
import { useTranslation } from 'react-i18next'
import { api } from '../../api'
import { useToast } from '../../hooks/useToast'
import { getErrorMessage } from '../../utils/error'
import type { AccountRow } from '../../types'

export function useAccountActions(reload: () => void, reloadSilently: () => void) {
  const { t } = useTranslation()
  const { showToast } = useToast()

  const handleAdd = useCallback(async (
    refreshToken: string,
    proxyUrl: string,
    setSubmitting: (loading: boolean) => void
  ) => {
    setSubmitting(true)
    try {
      const lines = refreshToken.split('\n').map(l => l.trim()).filter(Boolean)
      if (lines.length === 0) return

      if (lines.length === 1) {
        await api.addAccount({ refresh_token: lines[0], proxy_url: proxyUrl })
        showToast(t('accounts.addSuccess'))
      } else {
        const result = await api.batchImportAccounts({ refresh_tokens: lines, proxy_url: proxyUrl })
        const ok = result.results.filter(r => r.status === 'ok').length
        const fail = result.results.filter(r => r.status === 'error').length
        const dup = result.results.length === 0 && ok === 0 ? lines.length : 0
        showToast(t('accounts.batchImportDone', { success: ok, fail, duplicate: dup }))
      }
      reload()
      return true
    } catch (error) {
      showToast(t('accounts.addFailed', { error: getErrorMessage(error) }), 'error')
      return false
    } finally {
      setSubmitting(false)
    }
  }, [t, showToast, reload])

  const handleAddAT = useCallback(async (
    accessToken: string,
    proxyUrl: string,
    setSubmitting: (loading: boolean) => void
  ) => {
    setSubmitting(true)
    try {
      await api.addATAccount({ access_token: accessToken, proxy_url: proxyUrl })
      showToast(t('accounts.addSuccess'))
      reload()
      return true
    } catch (error) {
      showToast(t('accounts.addFailed', { error: getErrorMessage(error) }), 'error')
      return false
    } finally {
      setSubmitting(false)
    }
  }, [t, showToast, reload])

  const handleDelete = useCallback(async (account: AccountRow, confirm: any) => {
    const confirmed = await confirm({
      title: t('accounts.deleteTitle'),
      description: t('accounts.deleteDesc', { account: account.email || `ID ${account.id}` }),
      confirmText: t('accounts.deleteConfirm'),
      tone: 'destructive',
      confirmVariant: 'destructive',
    })
    if (!confirmed) return
    try {
      await api.deleteAccount(account.id)
      showToast(t('accounts.deleted'))
      reload()
    } catch (error) {
      showToast(t('accounts.deleteFailed', { error: getErrorMessage(error) }), 'error')
    }
  }, [t, showToast, reload])

  const handleRefresh = useCallback(async (
    account: AccountRow,
    addRefreshingId: (id: number) => void,
    removeRefreshingId: (id: number) => void
  ) => {
    addRefreshingId(account.id)
    try {
      const result = await api.refreshAccount(account.id)
      showToast(result.message || t('accounts.refreshRequested'))
      reloadSilently()
    } catch (error) {
      showToast(t('accounts.refreshFailed', { error: getErrorMessage(error) }), 'error')
    } finally {
      removeRefreshingId(account.id)
    }
  }, [t, showToast, reloadSilently])

  const handleBatchDelete = useCallback(async (
    selected: Set<number>,
    confirm: any,
    setBatchLoading: (loading: boolean) => void,
    clearSelection: () => void
  ) => {
    if (selected.size === 0) return
    const confirmed = await confirm({
      title: t('accounts.batchDeleteTitle'),
      description: t('accounts.batchDeleteDesc', { count: selected.size }),
      confirmText: t('accounts.deleteConfirm'),
      tone: 'destructive',
      confirmVariant: 'destructive',
    })
    if (!confirmed) return
    setBatchLoading(true)
    try {
      const res = await api.batchDeleteAccounts(Array.from(selected))
      showToast(t('accounts.batchDeleteDone', { success: res.deleted, fail: 0 }))
    } catch (error) {
      showToast(t('accounts.deleteFailed', { error: getErrorMessage(error) }), 'error')
    }
    clearSelection()
    setBatchLoading(false)
    reload()
  }, [t, showToast, reload])

  const handleBatchRefresh = useCallback(async (
    selected: Set<number>,
    setBatchLoading: (loading: boolean) => void
  ) => {
    if (selected.size === 0) return
    setBatchLoading(true)
    const results = await Promise.allSettled(
      [...selected].map((id) => api.refreshAccount(id))
    )
    const success = results.filter((r) => r.status === 'fulfilled').length
    const fail = results.length - success
    showToast(t('accounts.batchRefreshDone', { success, fail }))
    setBatchLoading(false)
    reload()
  }, [t, showToast, reload])

  const handleBatchTest = useCallback(async (
    setBatchTesting: (loading: boolean) => void,
    ids?: number[]
  ) => {
    setBatchTesting(true)
    try {
      const result = await api.batchTestAccounts(ids)
      showToast(t('accounts.batchTestDone', {
        success: result.success,
        banned: result.banned,
        rateLimited: result.rate_limited,
        failed: result.failed,
      }))
      reload()
    } catch (error) {
      showToast(t('accounts.batchTestFailed', { error: getErrorMessage(error) }), 'error')
    } finally {
      setBatchTesting(false)
    }
  }, [t, showToast, reload])

  const handleBatchRefreshAll = useCallback(async (
    setBatchRefreshing: (loading: boolean) => void
  ) => {
    setBatchRefreshing(true)
    try {
      const result = await api.batchRefreshAccounts()
      showToast(t('accounts.batchRefreshAllDone', {
        success: result.success,
        fail: result.fail,
        skipped: result.skipped,
      }))
      reload()
    } catch (error) {
      showToast(t('accounts.batchRefreshAllFailed', { error: getErrorMessage(error) }), 'error')
    } finally {
      setBatchRefreshing(false)
    }
  }, [t, showToast, reload])

  const handleCleanBanned = useCallback(async (
    confirm: any,
    setCleaningBanned: (loading: boolean) => void
  ) => {
    const confirmed = await confirm({
      title: t('accounts.cleanBannedTitle'),
      description: t('accounts.cleanBannedDesc'),
      confirmText: t('accounts.cleanConfirm'),
      tone: 'warning',
    })
    if (!confirmed) return
    setCleaningBanned(true)
    try {
      await api.cleanBanned()
      showToast(t('accounts.cleanBannedSuccess'))
      reload()
    } catch (error) {
      showToast(t('accounts.cleanBannedFailed', { error: getErrorMessage(error) }), 'error')
    } finally {
      setCleaningBanned(false)
    }
  }, [t, showToast, reload])

  const handleCleanRateLimited = useCallback(async (
    confirm: any,
    setCleaningRateLimited: (loading: boolean) => void
  ) => {
    const confirmed = await confirm({
      title: t('accounts.cleanRateLimitedTitle'),
      description: t('accounts.cleanRateLimitedDesc'),
      confirmText: t('accounts.cleanConfirm'),
      tone: 'warning',
    })
    if (!confirmed) return
    setCleaningRateLimited(true)
    try {
      await api.cleanRateLimited()
      showToast(t('accounts.cleanRateLimitedSuccess'))
      reload()
    } catch (error) {
      showToast(t('accounts.cleanRateLimitedFailed', { error: getErrorMessage(error) }), 'error')
    } finally {
      setCleaningRateLimited(false)
    }
  }, [t, showToast, reload])

  const handleCleanError = useCallback(async (
    confirm: any,
    setCleaningError: (loading: boolean) => void
  ) => {
    const confirmed = await confirm({
      title: t('accounts.cleanErrorTitle'),
      description: t('accounts.cleanErrorDesc'),
      confirmText: t('accounts.cleanConfirm'),
      tone: 'warning',
    })
    if (!confirmed) return
    setCleaningError(true)
    try {
      await api.cleanError()
      showToast(t('accounts.cleanErrorSuccess'))
      reload()
    } catch (error) {
      showToast(t('accounts.cleanErrorFailed', { error: getErrorMessage(error) }), 'error')
    } finally {
      setCleaningError(false)
    }
  }, [t, showToast, reload])

  return {
    handleAdd,
    handleAddAT,
    handleDelete,
    handleRefresh,
    handleBatchDelete,
    handleBatchRefresh,
    handleBatchTest,
    handleBatchRefreshAll,
    handleCleanBanned,
    handleCleanRateLimited,
    handleCleanError,
  }
}
