import type { ChangeEvent } from 'react'
import { useCallback, useEffect, useRef, useState } from 'react'
import { useTranslation } from 'react-i18next'
import { api, getAdminKey } from '../../api'
import PageHeader from '../../components/PageHeader'
import Pagination from '../../components/Pagination'
import StateShell from '../../components/StateShell'
import StatusBadge from '../../components/StatusBadge'
import ToastNotice from '../../components/ToastNotice'
import AccountUsageModal from '../../components/AccountUsageModal'
import { useDataLoader } from '../../hooks/useDataLoader'
import { useConfirmDialog } from '../../hooks/useConfirmDialog'
import { useToast } from '../../hooks/useToast'
import type { AccountRow } from '../../types'
import { getErrorMessage } from '../../utils/error'
import { formatRelativeTime, formatBeijingTime } from '../../utils/time'
import { Card, CardContent } from '@/components/ui/card'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '@/components/ui/table'
import { Plus, RefreshCw, Trash2, Zap, FlaskConical, Ban, Timer, AlertTriangle, Upload, Download, ArrowDownToLine, Search, BarChart3 } from 'lucide-react'

import { useAccountsState } from './useAccountsState'
import { useLoadingState } from './useLoadingState'
import { useModalState } from './useModalState'
import { useAddFormState } from './useAddFormState'
import { useAccountActions } from './useAccountActions'
import { filterAccounts, sortAccounts, calculateAccountStats } from './utils'
import { formatHealthTier, downloadBlob } from './formatters'
import { CompactStat, SchedulerChip } from './components/StatCards'
import { UsageCell } from './components/UsageCell'
import { TestConnectionModal } from './components/TestConnectionModal'
import { AddAccountModal } from './components/AddAccountModal'
import { ImportPickerModal, ExportPickerModal, ImportProgressModal } from './components/ImportExportModals'
import { MigrateModal } from './components/MigrateModal'

const PAGE_SIZE = 20

export default function Accounts() {
  const { t } = useTranslation()
  const { toast, showToast } = useToast()
  const { confirm, confirmDialog } = useConfirmDialog()

  // State hooks
  const accountsState = useAccountsState()
  const loadingState = useLoadingState()
  const modalState = useModalState()
  const addFormState = useAddFormState()

  // File input refs
  const fileInputRef = useRef<HTMLInputElement>(null)
  const jsonInputRef = useRef<HTMLInputElement>(null)
  const atFileInputRef = useRef<HTMLInputElement>(null)

  // Import/Export state
  const [importProgress, setImportProgress] = useState({ show: false, current: 0, total: 0, success: 0, duplicate: 0, failed: 0, done: false })
  const [migrateUrl, setMigrateUrl] = useState('')
  const [migrateKey, setMigrateKey] = useState('')

  // Load accounts
  const loadAccounts = useCallback(async () => {
    const data = await api.getAccounts()
    return data.accounts ?? []
  }, [])

  const { data: accounts, loading, error, reload, reloadSilently } = useDataLoader<AccountRow[]>({
    initialData: [],
    load: loadAccounts,
  })

  const usageBootstrapReloadedRef = useRef(false)

  useEffect(() => {
    const hasMissingUsage = accounts.some(
      (account) => account.plan_type?.toLowerCase() === 'free' && (account.usage_percent_7d === null || account.usage_percent_7d === undefined)
    )
    if (!hasMissingUsage || usageBootstrapReloadedRef.current) return

    usageBootstrapReloadedRef.current = true
    const timer = window.setTimeout(() => void reloadSilently(), 4000)
    return () => window.clearTimeout(timer)
  }, [accounts, reloadSilently])

  // Actions
  const actions = useAccountActions(reload, reloadSilently)

  // Calculate stats and filtered accounts
  const stats = calculateAccountStats(accounts)
  const filteredAccounts = filterAccounts(accounts, accountsState.statusFilter, accountsState.planFilter, accountsState.searchQuery)
  const sortedAccounts = sortAccounts(filteredAccounts, accountsState.sortKey, accountsState.sortDir)
  const totalPages = Math.max(1, Math.ceil(sortedAccounts.length / PAGE_SIZE))
  const pagedAccounts = sortedAccounts.slice((accountsState.page - 1) * PAGE_SIZE, accountsState.page * PAGE_SIZE)
  const allPageSelected = pagedAccounts.length > 0 && pagedAccounts.every((a) => accountsState.selected.has(a.id))

  const toggleSelectAll = () => {
    if (allPageSelected) {
      accountsState.setSelected((prev) => {
        const next = new Set(prev)
        for (const a of pagedAccounts) next.delete(a.id)
        return next
      })
    } else {
      accountsState.setSelected((prev) => {
        const next = new Set(prev)
        for (const a of pagedAccounts) next.add(a.id)
        return next
      })
    }
  }

  // Add account handlers
  const handleAddRT = async () => {
    const success = await actions.handleAdd(addFormState.addForm.refresh_token, addFormState.addForm.proxy_url, loadingState.setSubmitting)
    if (success) {
      modalState.setShowAdd(false)
      addFormState.resetAddForm()
    }
  }

  const handleAddAT = async () => {
    const success = await actions.handleAddAT(addFormState.atForm.access_token, addFormState.atForm.proxy_url, loadingState.setSubmitting)
    if (success) {
      modalState.setShowAdd(false)
      addFormState.resetAddForm()
    }
  }

  const handleOAuthGenerate = async () => {
    addFormState.setOauthGenerating(true)
    try {
      const result = await api.generateOAuthURL({ proxy_url: addFormState.oauthProxyUrl })
      addFormState.setOauthSession(result)
      addFormState.setOauthStep('exchange')
    } catch (error) {
      showToast(t('accounts.oauthFailed', { error: getErrorMessage(error) }), 'error')
    } finally {
      addFormState.setOauthGenerating(false)
    }
  }

  const handleOAuthComplete = async () => {
    if (!addFormState.oauthSession) return
    let code = ''
    let state = ''
    const raw = addFormState.oauthCallbackUrl.trim()
    try {
      const url = new URL(raw)
      code = url.searchParams.get('code') ?? ''
      state = url.searchParams.get('state') ?? ''
    } catch {
      const qs = raw.includes('?') ? raw.split('?')[1] : raw
      const params = new URLSearchParams(qs)
      code = params.get('code') ?? ''
      state = params.get('state') ?? ''
    }
    if (!code || !state) {
      showToast(t('accounts.oauthParseError'), 'error')
      return
    }
    addFormState.setOauthCompleting(true)
    try {
      const result = await api.exchangeOAuthCode({
        session_id: addFormState.oauthSession.session_id,
        code,
        state,
        name: addFormState.oauthName.trim() || undefined,
        proxy_url: addFormState.oauthProxyUrl.trim() || undefined,
      })
      showToast(result.email ? t('accounts.oauthSuccess', { email: result.email }) : t('accounts.oauthSuccessNoEmail'))
      modalState.setShowAdd(false)
      addFormState.resetAddForm()
      void reload()
    } catch (error) {
      showToast(t('accounts.oauthFailed', { error: getErrorMessage(error) }), 'error')
    } finally {
      addFormState.setOauthCompleting(false)
    }
  }

  // Import/Export handlers
  const readImportSSE = async (res: Response) => {
    setImportProgress({ show: true, current: 0, total: 0, success: 0, duplicate: 0, failed: 0, done: false })
    const reader = res.body?.getReader()
    if (!reader) return
    const decoder = new TextDecoder()
    let buffer = ''
    for (;;) {
      const { done, value } = await reader.read()
      if (done) break
      buffer += decoder.decode(value, { stream: true })
      const lines = buffer.split('\n')
      buffer = lines.pop() ?? ''
      for (const line of lines) {
        if (!line.startsWith('data: ')) continue
        try {
          const event = JSON.parse(line.slice(6)) as { type: string; current: number; total: number; success: number; duplicate: number; failed: number }
          setImportProgress(p => ({ ...p, current: event.current, total: event.total, success: event.success, duplicate: event.duplicate, failed: event.failed, done: event.type === 'complete' }))
          if (event.type === 'complete') void reload()
        } catch { /* ignore */ }
      }
    }
  }

  const handleFileImport = async (event: ChangeEvent<HTMLInputElement>) => {
    const file = event.target.files?.[0]
    if (!file) return
    if (!file.name.endsWith('.txt')) {
      showToast(t('accounts.selectTxtFile'), 'error')
      return
    }
    loadingState.setImporting(true)
    modalState.setShowImportPicker(false)
    try {
      const formData = new FormData()
      formData.append('file', file)
      const res = await fetch('/api/admin/accounts/import', { method: 'POST', body: formData, headers: getAdminKey() ? { 'X-Admin-Key': getAdminKey() } : {} })
      if (res.headers.get('content-type')?.includes('text/event-stream')) {
        await readImportSSE(res)
      } else {
        const data = await res.json()
        if (!res.ok) {
          showToast(data.error ? t('accounts.importFailedWithReason', { error: data.error }) : t('accounts.importFailed'), 'error')
        } else {
          showToast(t('accounts.importCompleted'))
          void reload()
        }
      }
    } catch (error) {
      showToast(t('accounts.importFailedWithReason', { error: getErrorMessage(error) }), 'error')
    } finally {
      loadingState.setImporting(false)
      if (fileInputRef.current) fileInputRef.current.value = ''
    }
  }

  const handleJsonImport = async (event: ChangeEvent<HTMLInputElement>) => {
    const files = event.target.files
    if (!files || files.length === 0) return
    loadingState.setImporting(true)
    modalState.setShowImportPicker(false)
    try {
      const formData = new FormData()
      formData.append('format', 'json')
      for (let i = 0; i < files.length; i++) {
        formData.append('file', files[i])
      }
      const res = await fetch('/api/admin/accounts/import', { method: 'POST', body: formData, headers: getAdminKey() ? { 'X-Admin-Key': getAdminKey() } : {} })
      if (res.headers.get('content-type')?.includes('text/event-stream')) {
        await readImportSSE(res)
      } else {
        const data = await res.json()
        if (!res.ok) {
          showToast(data.error ? t('accounts.importFailedWithReason', { error: data.error }) : t('accounts.importFailed'), 'error')
        } else {
          showToast(t('accounts.importCompleted'))
          void reload()
        }
      }
    } catch (error) {
      showToast(t('accounts.importFailedWithReason', { error: getErrorMessage(error) }), 'error')
    } finally {
      loadingState.setImporting(false)
      if (jsonInputRef.current) jsonInputRef.current.value = ''
    }
  }

  const handleAtFileImport = async (event: ChangeEvent<HTMLInputElement>) => {
    const file = event.target.files?.[0]
    if (!file) return
    if (!file.name.endsWith('.txt')) {
      showToast(t('accounts.selectTxtFile'), 'error')
      return
    }
    loadingState.setImporting(true)
    modalState.setShowImportPicker(false)
    try {
      const formData = new FormData()
      formData.append('file', file)
      formData.append('format', 'at_txt')
      const res = await fetch('/api/admin/accounts/import', { method: 'POST', body: formData, headers: getAdminKey() ? { 'X-Admin-Key': getAdminKey() } : {} })
      if (res.headers.get('content-type')?.includes('text/event-stream')) {
        await readImportSSE(res)
      } else {
        const data = await res.json()
        if (!res.ok) {
          showToast(data.error ? t('accounts.importFailedWithReason', { error: data.error }) : t('accounts.importFailed'), 'error')
        } else {
          showToast(t('accounts.importCompleted'))
          void reload()
        }
      }
    } catch (error) {
      showToast(t('accounts.importFailedWithReason', { error: getErrorMessage(error) }), 'error')
    } finally {
      loadingState.setImporting(false)
      if (atFileInputRef.current) atFileInputRef.current.value = ''
    }
  }

  const handleExport = async (format: 'json' | 'txt', scope: 'healthy' | 'selected') => {
    loadingState.setExporting(true)
    modalState.setShowExportPicker(false)
    try {
      const params: { filter: 'healthy' | 'all'; ids?: number[] } = {
        filter: scope === 'healthy' ? 'healthy' : 'all',
      }
      if (scope === 'selected') {
        params.ids = Array.from(accountsState.selected)
        params.filter = 'all'
      }
      const data = await api.exportAccounts(params)
      if (data.length === 0) {
        showToast(t('accounts.exportNoAccounts'), 'error')
        return
      }
      const ts = new Date().toISOString().replace(/[:.]/g, '-').slice(0, 19)
      if (format === 'json') {
        const blob = new Blob([JSON.stringify(data, null, 2)], { type: 'application/json' })
        downloadBlob(blob, `cpa-${ts}-${data.length}.json`)
      } else {
        const text = data.map(e => e.refresh_token).join('\n')
        const blob = new Blob([text], { type: 'text/plain' })
        downloadBlob(blob, `rt-${ts}-${data.length}.txt`)
      }
      showToast(t('accounts.exportSuccess', { count: data.length }))
    } catch (error) {
      showToast(`${t('accounts.exportFailed')}: ${getErrorMessage(error)}`, 'error')
    } finally {
      loadingState.setExporting(false)
    }
  }

  const handleMigrate = async () => {
    loadingState.setMigrating(true)
    modalState.setShowMigrate(false)
    try {
      const res = await fetch('/api/admin/accounts/migrate', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', ...(getAdminKey() ? { 'X-Admin-Key': getAdminKey() } : {}) },
        body: JSON.stringify({ url: migrateUrl.trim(), admin_key: migrateKey.trim() }),
      })
      if (res.headers.get('content-type')?.includes('text/event-stream')) {
        await readImportSSE(res)
      } else {
        const data = await res.json()
        if (!res.ok) {
          showToast(data.error ? `${t('accounts.migrateFailed')}: ${data.error}` : t('accounts.migrateFailed'), 'error')
        } else {
          showToast(t('accounts.migrateSuccess', { imported: data.imported ?? 0, duplicate: data.duplicate ?? 0, failed: data.failed ?? 0 }))
          void reload()
        }
      }
    } catch (error) {
      showToast(`${t('accounts.migrateFailed')}: ${getErrorMessage(error)}`, 'error')
    } finally {
      loadingState.setMigrating(false)
      setMigrateUrl('')
      setMigrateKey('')
    }
  }

  return (
    <StateShell
      variant="page"
      loading={loading}
      error={error}
      onRetry={() => void reload()}
      loadingTitle={t('accounts.loadingTitle')}
      loadingDescription={t('accounts.loadingDesc')}
      errorTitle={t('accounts.errorTitle')}
    >
      <>
        <PageHeader
          title={t('accounts.title')}
          description={t('accounts.description')}
          onRefresh={() => void reload()}
          actions={(
            <div className="flex items-center gap-1.5">
              <Button variant="outline" size="sm" disabled={loadingState.batchTesting} onClick={() => void actions.handleBatchTest(loadingState.setBatchTesting)}>
                <FlaskConical className="size-3" />
                {loadingState.batchTesting ? t('accounts.batchTesting') : t('accounts.batchTest')}
              </Button>
              <Button variant="outline" size="sm" disabled={loadingState.batchRefreshing} onClick={() => void actions.handleBatchRefreshAll(loadingState.setBatchRefreshing)}>
                <RefreshCw className={`size-3 ${loadingState.batchRefreshing ? 'animate-spin' : ''}`} />
                {loadingState.batchRefreshing ? t('accounts.batchRefreshing') : t('accounts.batchRefreshAll')}
              </Button>
              <Button variant="outline" size="sm" disabled={loadingState.cleaningBanned} onClick={() => void actions.handleCleanBanned(confirm, loadingState.setCleaningBanned)}>
                <Ban className="size-3" />
                {loadingState.cleaningBanned ? t('accounts.cleaning') : t('accounts.cleanBanned')}
              </Button>
              <Button variant="outline" size="sm" disabled={loadingState.cleaningRateLimited} onClick={() => void actions.handleCleanRateLimited(confirm, loadingState.setCleaningRateLimited)}>
                <Timer className="size-3" />
                {loadingState.cleaningRateLimited ? t('accounts.cleaning') : t('accounts.cleanRateLimited')}
              </Button>
              <Button variant="outline" size="sm" disabled={loadingState.cleaningError} onClick={() => void actions.handleCleanError(confirm, loadingState.setCleaningError)}>
                <AlertTriangle className="size-3" />
                {loadingState.cleaningError ? t('accounts.cleaning') : t('accounts.cleanError')}
              </Button>
              <Button onClick={() => modalState.setShowAdd(true)}>
                <Plus className="size-3.5" />
                {t('accounts.addAccount')}
              </Button>
              <Button variant="outline" disabled={loadingState.importing} onClick={() => modalState.setShowImportPicker(true)}>
                <Upload className="size-3.5" />
                {loadingState.importing ? t('accounts.importing') : t('accounts.importFile')}
              </Button>
              <Button variant="outline" disabled={loadingState.exporting} onClick={() => modalState.setShowExportPicker(true)}>
                <Download className="size-3.5" />
                {loadingState.exporting ? t('accounts.exporting') : t('accounts.export')}
              </Button>
              <Button variant="outline" disabled={loadingState.migrating} onClick={() => modalState.setShowMigrate(true)}>
                <ArrowDownToLine className="size-3.5" />
                {loadingState.migrating ? t('accounts.migrating') : t('accounts.migrateImport')}
              </Button>
              <input ref={fileInputRef} type="file" accept=".txt" className="hidden" onChange={(e) => void handleFileImport(e)} />
              <input ref={jsonInputRef} type="file" accept=".json" multiple className="hidden" onChange={(e) => void handleJsonImport(e)} />
              <input ref={atFileInputRef} type="file" accept=".txt" className="hidden" onChange={(e) => void handleAtFileImport(e)} />
            </div>
          )}
        />

        <div className="mb-4 grid grid-cols-2 gap-3 xl:grid-cols-4">
          <CompactStat label={t('accounts.totalAccounts')} chipLabel={t('accounts.filterAll')} value={stats.total} tone="neutral" />
          <CompactStat label={t('accounts.normalAccounts')} chipLabel={t('accounts.filterNormal')} value={stats.normal} tone="success" />
          <CompactStat label={t('accounts.rateLimited')} chipLabel={t('accounts.filterRateLimited')} value={stats.rateLimited} tone="warning" />
          <CompactStat label={t('accounts.bannedAccounts')} chipLabel={t('accounts.filterBanned')} value={stats.banned} tone="danger" />
        </div>

        <div className="mb-4 flex flex-wrap items-center gap-2 rounded-2xl border border-border bg-white/55 px-4 py-3 text-[12px] text-muted-foreground shadow-[inset_0_1px_0_rgba(255,255,255,0.72)]">
          <span className="font-semibold text-foreground">{t('accounts.filter')}</span>
          {([['all', t('accounts.filterAll')], ['normal', t('accounts.filterNormal')], ['rate_limited', t('accounts.filterRateLimited')], ['banned', t('accounts.filterBanned')]] as const).map(([key, label]) => (
            <button
              key={key}
              onClick={() => { accountsState.setStatusFilter(key); accountsState.setPage(1) }}
              className={`rounded-full px-3 py-1 font-semibold transition-colors ${
                accountsState.statusFilter === key
                  ? 'bg-primary text-primary-foreground'
                  : 'bg-muted/50 text-muted-foreground hover:bg-muted'
              }`}
            >
              {label} {key === 'all' ? stats.total : key === 'normal' ? stats.normal : key === 'rate_limited' ? stats.rateLimited : stats.banned}
            </button>
          ))}
        </div>

        <div className="mb-4 flex flex-wrap items-center gap-2 rounded-2xl border border-border bg-white/55 px-4 py-3 text-[12px] text-muted-foreground shadow-[inset_0_1px_0_rgba(255,255,255,0.72)]">
          <span className="font-semibold text-foreground">{t('accounts.schedulerView')}</span>
          <SchedulerChip label={t('accounts.healthy')} value={stats.healthy} tone="success" />
          <SchedulerChip label={t('accounts.warm')} value={stats.warm} tone="warning" />
          <SchedulerChip label={t('accounts.risky')} value={stats.risky} tone="danger" />
          <SchedulerChip label={t('status.unauthorized')} value={stats.banned} tone="neutral" />
        </div>

        <div className="mb-4 flex items-center gap-2">
          <div className="relative w-64">
            <Search className="absolute left-3 top-1/2 -translate-y-1/2 size-4 text-muted-foreground pointer-events-none" />
            <Input
              className="pl-9 h-8 rounded-lg text-[13px]"
              placeholder={t('accounts.searchPlaceholder')}
              value={accountsState.searchQuery}
              onChange={(e: ChangeEvent<HTMLInputElement>) => { accountsState.setSearchQuery(e.target.value); accountsState.setPage(1) }}
            />
          </div>
          <div className="flex items-center gap-1 rounded-lg border border-border bg-muted/30 p-0.5">
            {(['all', 'pro', 'team', 'free'] as const).map((key) => (
              <button
                key={key}
                onClick={() => { accountsState.setPlanFilter(key); accountsState.setPage(1) }}
                className={`rounded-md px-2.5 py-1 text-[12px] font-medium transition-colors ${
                  accountsState.planFilter === key
                    ? 'bg-background shadow-sm text-foreground'
                    : 'text-muted-foreground hover:text-foreground'
                }`}
              >
                {key === 'all' ? t('accounts.filterAll') : key.charAt(0).toUpperCase() + key.slice(1)}
              </button>
            ))}
          </div>
        </div>

        {accountsState.selected.size > 0 && (
          <div className="flex items-center justify-between gap-3 px-4 py-2.5 mb-4 rounded-2xl bg-primary/10 border border-primary/20 text-sm font-semibold text-primary">
            <span>{t('common.selected', { count: accountsState.selected.size })}</span>
            <div className="flex items-center gap-1.5">
              <Button variant="outline" size="sm" disabled={loadingState.batchLoading || loadingState.batchTesting} onClick={() => void actions.handleBatchTest(loadingState.setBatchTesting, [...accountsState.selected])}>
                <FlaskConical className="size-3" />
                {loadingState.batchTesting ? t('accounts.batchTesting') : t('accounts.batchTest')}
              </Button>
              <Button variant="outline" size="sm" disabled={loadingState.batchLoading} onClick={() => void actions.handleBatchRefresh(accountsState.selected, loadingState.setBatchLoading)}>
                {t('accounts.batchRefresh')}
              </Button>
              <Button variant="destructive" size="sm" disabled={loadingState.batchLoading} onClick={() => void actions.handleBatchDelete(accountsState.selected, confirm, loadingState.setBatchLoading, accountsState.clearSelection)}>
                {t('accounts.batchDelete')}
              </Button>
              <Button variant="outline" size="sm" onClick={() => accountsState.clearSelection()}>
                {t('accounts.cancelSelection')}
              </Button>
            </div>
          </div>
        )}

        <Card>
          <CardContent className="p-6">
            <StateShell
              variant="section"
              isEmpty={accounts.length === 0}
              emptyTitle={t('accounts.noData')}
              emptyDescription={t('accounts.noDataDesc')}
              action={<Button onClick={() => modalState.setShowAdd(true)}>{t('accounts.addAccount')}</Button>}
            >
              <div className="overflow-auto border border-border rounded-xl">
                <Table>
                  <TableHeader>
                    <TableRow>
                      <TableHead className="w-10">
                        <input
                          type="checkbox"
                          className="size-4 cursor-pointer accent-[hsl(var(--primary))]"
                          checked={allPageSelected}
                          onChange={toggleSelectAll}
                        />
                      </TableHead>
                      <TableHead className="text-[13px] font-semibold">ID</TableHead>
                      <TableHead className="text-[13px] font-semibold">{t('accounts.email')}</TableHead>
                      <TableHead className="text-[13px] font-semibold">{t('accounts.plan')}</TableHead>
                      <TableHead className="text-[13px] font-semibold">{t('accounts.status')}</TableHead>
                      <TableHead
                        className="text-[13px] font-semibold cursor-pointer select-none hover:text-primary transition-colors"
                        onClick={() => { if (accountsState.sortKey === 'requests') { accountsState.setSortDir(d => d === 'asc' ? 'desc' : 'asc') } else { accountsState.setSortKey('requests'); accountsState.setSortDir('desc') }; accountsState.setPage(1) }}
                      >
                        {t('accounts.requests')} {accountsState.sortKey === 'requests' ? (accountsState.sortDir === 'desc' ? '↓' : '↑') : ''}
                      </TableHead>
                      <TableHead
                        className="text-[13px] font-semibold cursor-pointer select-none hover:text-primary transition-colors"
                        onClick={() => { if (accountsState.sortKey === 'usage') { accountsState.setSortDir(d => d === 'asc' ? 'desc' : 'asc') } else { accountsState.setSortKey('usage'); accountsState.setSortDir('desc') }; accountsState.setPage(1) }}
                      >
                        {t('accounts.usage')} {accountsState.sortKey === 'usage' ? (accountsState.sortDir === 'desc' ? '↓' : '↑') : ''}
                      </TableHead>
                      <TableHead
                        className="text-[13px] font-semibold cursor-pointer select-none hover:text-primary transition-colors"
                        onClick={() => { if (accountsState.sortKey === 'importTime') { accountsState.setSortDir(d => d === 'asc' ? 'desc' : 'asc') } else { accountsState.setSortKey('importTime'); accountsState.setSortDir('desc') }; accountsState.setPage(1) }}
                      >
                        {t('accounts.importTime')} {accountsState.sortKey === 'importTime' ? (accountsState.sortDir === 'desc' ? '↓' : '↑') : ''}
                      </TableHead>
                      <TableHead className="text-[13px] font-semibold">{t('accounts.updatedAt')}</TableHead>
                      <TableHead className="text-[13px] font-semibold text-right">{t('accounts.actions')}</TableHead>
                    </TableRow>
                  </TableHeader>
                  <TableBody>
                    {pagedAccounts.map((account) => (
                      <TableRow key={account.id} className={accountsState.selected.has(account.id) ? 'bg-primary/5' : ''}>
                        <TableCell>
                          <input
                            type="checkbox"
                            className="size-4 cursor-pointer accent-[hsl(var(--primary))]"
                            checked={accountsState.selected.has(account.id)}
                            onChange={() => accountsState.toggleSelect(account.id)}
                          />
                        </TableCell>
                        <TableCell className="text-[14px] font-mono text-muted-foreground">{account.id}</TableCell>
                        <TableCell className="text-[14px] text-muted-foreground">
                          {account.email || '-'}
                          {account.at_only && (
                            <span className="ml-1.5 inline-flex items-center rounded-md bg-amber-50 px-1.5 py-0.5 text-[10px] font-medium text-amber-700 ring-1 ring-inset ring-amber-600/20 dark:bg-amber-950 dark:text-amber-400 dark:ring-amber-400/20">
                              AT
                            </span>
                          )}
                        </TableCell>
                        <TableCell className="text-[13px] font-medium">
                          {account.plan_type || '-'}
                        </TableCell>
                        <TableCell>
                          <div className="space-y-1">
                            <StatusBadge status={account.status} />
                            <div className="text-[11px] text-muted-foreground">
                              {t('accounts.healthSummary', {
                                health: formatHealthTier(account.health_tier, t),
                                score: Math.round(account.scheduler_score ?? 0),
                                concurrency: account.dynamic_concurrency_limit ?? '-',
                              })}
                            </div>
                          </div>
                        </TableCell>
                        <TableCell>
                          <div className="flex items-center gap-2 text-[13px]">
                            <span className="text-emerald-600 font-medium">{account.success_requests ?? 0}</span>
                            <span className="text-muted-foreground">/</span>
                            <span className="text-red-500 font-medium">{account.error_requests ?? 0}</span>
                          </div>
                        </TableCell>
                        <TableCell>
                          <UsageCell account={account} />
                        </TableCell>
                        <TableCell className="text-[13px] text-muted-foreground whitespace-nowrap">{formatBeijingTime(account.created_at)}</TableCell>
                        <TableCell className="text-[14px] text-muted-foreground">{formatRelativeTime(account.updated_at)}</TableCell>
                        <TableCell className="text-right">
                          <div className="flex items-center gap-1 justify-end">
                            <Button
                              variant="outline"
                              size="icon"
                              className="h-7 w-8 px-0"
                              onClick={() => accountsState.setUsageAccount(account)}
                              title={t('accounts.usageDetail')}
                            >
                              <BarChart3 className="size-3.5" />
                            </Button>
                            <Button
                              variant="outline"
                              size="icon"
                              className="h-7 w-8 px-0"
                              onClick={() => accountsState.setTestingAccount(account)}
                              title={t('accounts.testConnection')}
                            >
                              <Zap className="size-3.5" />
                            </Button>
                            <Button
                              variant="outline"
                              size="icon"
                              className="h-7 w-8 px-0"
                              disabled={accountsState.refreshingIds.has(account.id) || account.at_only}
                              onClick={() => void actions.handleRefresh(account, accountsState.addRefreshingId, accountsState.removeRefreshingId)}
                              title={account.at_only ? t('accounts.atRefreshDisabled') : t('accounts.refreshAccessToken')}
                            >
                              <RefreshCw className={`size-3.5 ${accountsState.refreshingIds.has(account.id) ? 'animate-spin' : ''}`} />
                            </Button>
                            <Button
                              variant="destructive"
                              size="icon"
                              className="h-7 w-8 px-0"
                              onClick={() => void actions.handleDelete(account, confirm)}
                              title={t('accounts.deleteAccount')}
                            >
                              <Trash2 className="size-3.5" />
                            </Button>
                          </div>
                        </TableCell>
                      </TableRow>
                    ))}
                  </TableBody>
                </Table>
              </div>
              <Pagination
                page={accountsState.page}
                totalPages={totalPages}
                onPageChange={accountsState.setPage}
                totalItems={accounts.length}
                pageSize={PAGE_SIZE}
              />
            </StateShell>
          </CardContent>
        </Card>

        <AddAccountModal
          show={modalState.showAdd}
          onClose={() => {
            modalState.setShowAdd(false)
            addFormState.resetAddForm()
          }}
          addMethod={addFormState.addMethod}
          setAddMethod={addFormState.setAddMethod}
          rtForm={addFormState.addForm}
          setRtForm={addFormState.setAddForm}
          atForm={addFormState.atForm}
          setAtForm={addFormState.setAtForm}
          oauthStep={addFormState.oauthStep}
          setOauthStep={addFormState.setOauthStep}
          oauthSession={addFormState.oauthSession}
          oauthProxyUrl={addFormState.oauthProxyUrl}
          setOauthProxyUrl={addFormState.setOauthProxyUrl}
          oauthCallbackUrl={addFormState.oauthCallbackUrl}
          setOauthCallbackUrl={addFormState.setOauthCallbackUrl}
          oauthName={addFormState.oauthName}
          setOauthName={addFormState.setOauthName}
          oauthGenerating={addFormState.oauthGenerating}
          oauthCompleting={addFormState.oauthCompleting}
          submitting={loadingState.submitting}
          onSubmitRT={handleAddRT}
          onSubmitAT={handleAddAT}
          onOAuthGenerate={handleOAuthGenerate}
          onOAuthComplete={handleOAuthComplete}
        />

        <ImportPickerModal
          show={modalState.showImportPicker}
          onClose={() => modalState.setShowImportPicker(false)}
          onSelectTxt={() => fileInputRef.current?.click()}
          onSelectJson={() => jsonInputRef.current?.click()}
          onSelectAtTxt={() => atFileInputRef.current?.click()}
        />

        <ExportPickerModal
          show={modalState.showExportPicker}
          onClose={() => modalState.setShowExportPicker(false)}
          selectedCount={accountsState.selected.size}
          onExport={handleExport}
        />

        <MigrateModal
          show={modalState.showMigrate}
          onClose={() => {
            modalState.setShowMigrate(false)
            setMigrateUrl('')
            setMigrateKey('')
          }}
          url={migrateUrl}
          setUrl={setMigrateUrl}
          adminKey={migrateKey}
          setAdminKey={setMigrateKey}
          migrating={loadingState.migrating}
          onConfirm={handleMigrate}
        />

        {accountsState.testingAccount && (
          <TestConnectionModal
            account={accountsState.testingAccount}
            onSettled={() => void reloadSilently()}
            onClose={() => accountsState.setTestingAccount(null)}
          />
        )}

        {accountsState.usageAccount && (
          <AccountUsageModal
            account={accountsState.usageAccount}
            onClose={() => accountsState.setUsageAccount(null)}
          />
        )}

        <ImportProgressModal
          show={importProgress.show}
          done={importProgress.done}
          current={importProgress.current}
          total={importProgress.total}
          success={importProgress.success}
          duplicate={importProgress.duplicate}
          failed={importProgress.failed}
          onClose={() => setImportProgress(p => ({ ...p, show: false }))}
        />

        {confirmDialog}

        <ToastNotice toast={toast} />
      </>
    </StateShell>
  )
}
