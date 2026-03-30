import { useState, useEffect, useCallback } from 'react'
import { useTranslation } from 'react-i18next'
import { Globe, Plus, Trash2, Play, MapPin, Loader2, Zap, ChevronLeft, ChevronRight, Eye, EyeOff } from 'lucide-react'
import { Card, CardContent } from '@/components/ui/card'
import { api, type ProxyRow, type ProxyTestResult } from '../api'

const PAGE_SIZE = 10

function latencyColor(ms: number): string {
  if (ms <= 0) return 'text-muted-foreground'
  if (ms < 500) return 'text-emerald-600 dark:text-emerald-400'
  if (ms < 1500) return 'text-amber-600 dark:text-amber-400'
  return 'text-red-600 dark:text-red-400'
}

function latencyBg(ms: number): string {
  if (ms <= 0) return ''
  if (ms < 500) return 'bg-emerald-500/10'
  if (ms < 1500) return 'bg-amber-500/10'
  return 'bg-red-500/10'
}

function maskUrl(url: string): string {
  try {
    const u = new URL(url)
    const host = u.hostname
    const masked = host.length > 6 ? host.slice(0, 3) + '***' + host.slice(-3) : '***'
    return `${u.protocol}//${u.username ? '***:***@' : ''}${masked}${u.port ? ':' + u.port : ''}`
  } catch {
    return url.slice(0, 10) + '******'
  }
}

export default function Proxies() {
  const { t, i18n } = useTranslation()
  const [proxies, setProxies] = useState<ProxyRow[]>([])
  const [loading, setLoading] = useState(true)
  const [poolEnabled, setPoolEnabled] = useState(false)
  const [showAdd, setShowAdd] = useState(false)
  const [addInput, setAddInput] = useState('')
  const [addLabel, setAddLabel] = useState('')
  const [addLoading, setAddLoading] = useState(false)
  const [selected, setSelected] = useState<Set<number>>(new Set())
  const [testingIds, setTestingIds] = useState<Set<number>>(new Set())
  const [testAllLoading, setTestAllLoading] = useState(false)
  const [page, setPage] = useState(1)
  const [revealedIds, setRevealedIds] = useState<Set<number>>(new Set())

  const ipApiLang = i18n.language?.startsWith('zh') ? 'zh-CN' : 'en'

  const reload = useCallback(async () => {
    try {
      const [proxyRes, settingsRes] = await Promise.all([api.listProxies(), api.getSettings()])
      setProxies(proxyRes.proxies)
      setPoolEnabled(settingsRes.proxy_pool_enabled)
    } catch { /* ignore */ }
    setLoading(false)
  }, [])

  useEffect(() => { reload() }, [reload])

  const totalPages = Math.max(1, Math.ceil(proxies.length / PAGE_SIZE))
  const pagedProxies = proxies.slice((page - 1) * PAGE_SIZE, page * PAGE_SIZE)

  useEffect(() => {
    if (page > totalPages) setPage(totalPages)
  }, [page, totalPages])

  const handleTogglePool = async () => {
    const next = !poolEnabled
    setPoolEnabled(next)
    try {
      await api.updateSettings({ proxy_pool_enabled: next })
    } catch {
      setPoolEnabled(!next)
    }
  }

  const handleAdd = async () => {
    const urls = addInput.split('\n').map(s => s.trim()).filter(Boolean)
    if (urls.length === 0) return
    setAddLoading(true)
    try {
      await api.addProxies({ urls, label: addLabel })
      setAddInput('')
      setAddLabel('')
      setShowAdd(false)
      await reload()
    } catch { /* ignore */ }
    setAddLoading(false)
  }

  const handleDelete = async (id: number) => {
    try {
      await api.deleteProxy(id)
      await reload()
    } catch { /* ignore */ }
  }

  const handleBatchDelete = async () => {
    if (selected.size === 0) return
    try {
      await api.batchDeleteProxies([...selected])
      setSelected(new Set())
      await reload()
    } catch { /* ignore */ }
  }

  const handleToggle = async (p: ProxyRow) => {
    try {
      await api.updateProxy(p.id, { enabled: !p.enabled })
      await reload()
    } catch { /* ignore */ }
  }

  const handleTest = async (p: ProxyRow) => {
    setTestingIds(prev => new Set(prev).add(p.id))
    try {
      const result = await api.testProxy(p.url, p.id, ipApiLang)
      if (result.success) {
        setProxies(prev => prev.map(px =>
          px.id === p.id
            ? { ...px, test_ip: result.ip || '', test_location: result.location || '', test_latency_ms: result.latency_ms || 0 }
            : px
        ))
      }
    } catch { /* ignore */ }
    setTestingIds(prev => {
      const next = new Set(prev)
      next.delete(p.id)
      return next
    })
  }

  const handleTestAll = async () => {
    setTestAllLoading(true)
    for (const p of proxies) {
      setTestingIds(prev => new Set(prev).add(p.id))
      try {
        const result = await api.testProxy(p.url, p.id, ipApiLang)
        if (result.success) {
          setProxies(prev => prev.map(px =>
            px.id === p.id
              ? { ...px, test_ip: result.ip || '', test_location: result.location || '', test_latency_ms: result.latency_ms || 0 }
              : px
          ))
        }
      } catch { /* ignore */ }
      setTestingIds(prev => {
        const next = new Set(prev)
        next.delete(p.id)
        return next
      })
    }
    setTestAllLoading(false)
  }

  const allSelected = pagedProxies.length > 0 && pagedProxies.every(p => selected.has(p.id))
  const toggleSelectAll = () => {
    if (allSelected) {
      setSelected(prev => {
        const next = new Set(prev)
        pagedProxies.forEach(p => next.delete(p.id))
        return next
      })
    } else {
      setSelected(prev => {
        const next = new Set(prev)
        pagedProxies.forEach(p => next.add(p.id))
        return next
      })
    }
  }

  const enabledCount = proxies.filter(p => p.enabled).length
  const canEnable = enabledCount > 0

  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="flex items-start justify-between gap-4 flex-wrap">
        <div>
          <h2 className="text-2xl font-bold text-foreground flex items-center gap-2.5">
            <Globe className="size-6 text-primary" />
            {t('nav.proxies')}
          </h2>
          <p className="mt-1 text-sm text-muted-foreground">
            {t('proxies.description')}
          </p>
        </div>
        <div className="flex items-center gap-3">
          {/* Pool Toggle Switch */}
          <div className="flex items-center gap-3" title={!canEnable && !poolEnabled ? t('proxies.addFirstProxy') : undefined}>
            <span className={`text-sm font-medium ${poolEnabled ? 'text-emerald-600 dark:text-emerald-400' : 'text-muted-foreground'}`}>
              {poolEnabled ? t('proxies.poolEnabled') : t('proxies.poolDisabled')}
            </span>
            <button
              role="switch"
              aria-checked={poolEnabled}
              disabled={!canEnable && !poolEnabled}
              onClick={handleTogglePool}
              className={`relative inline-flex h-6 w-11 shrink-0 cursor-pointer items-center rounded-full border-2 border-transparent transition-colors duration-200 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-primary/50 disabled:cursor-not-allowed disabled:opacity-40 ${
                poolEnabled ? 'bg-emerald-500' : 'bg-muted-foreground/30'
              }`}
            >
              <span className={`pointer-events-none inline-block size-5 transform rounded-full bg-white shadow-lg ring-0 transition-transform duration-200 ${poolEnabled ? 'translate-x-5' : 'translate-x-0'}`} />
            </button>
          </div>

          {selected.size > 0 && (
            <button
              onClick={handleBatchDelete}
              className="flex items-center gap-2 px-4 py-2 rounded-xl text-sm font-semibold bg-destructive/10 border border-destructive/30 text-destructive hover:bg-destructive/20 transition-all"
            >
              <Trash2 className="size-4" />
              {t('proxies.deleteSelected', { count: selected.size })}
            </button>
          )}

          {proxies.length > 0 && (
            <button
              onClick={handleTestAll}
              disabled={testAllLoading}
              className="flex items-center gap-2 px-4 py-2 rounded-xl text-sm font-semibold border border-border text-foreground hover:bg-muted/50 transition-all disabled:opacity-50"
            >
              {testAllLoading ? <Loader2 className="size-4 animate-spin" /> : <Zap className="size-4" />}
              {testAllLoading ? t('proxies.testingAll') : t('proxies.testAll')}
            </button>
          )}

          <button
            onClick={() => setShowAdd(!showAdd)}
            className="flex items-center gap-2 px-4 py-2 rounded-xl text-sm font-semibold bg-primary text-primary-foreground hover:opacity-90 transition-opacity shadow-sm"
          >
            <Plus className="size-4" />
            {t('proxies.addProxy')}
          </button>
        </div>
      </div>

      {/* Add Panel */}
      {showAdd && (
        <Card className="py-0">
          <CardContent className="p-6 space-y-4">
            <h4 className="text-base font-semibold text-foreground">{t('proxies.addProxyTitle')}</h4>
            <p className="text-sm text-muted-foreground">
              {t('proxies.addProxyDesc')}
            </p>
            <textarea
              value={addInput}
              onChange={e => setAddInput(e.target.value)}
              placeholder={"http://user:pass@ip:port\nsocks5://ip:port"}
              className="w-full h-32 px-3 py-2 text-sm rounded-xl border border-border bg-background text-foreground placeholder:text-muted-foreground resize-none outline-none focus:ring-2 focus:ring-primary/30 font-mono"
            />
            <div className="flex items-center gap-3">
              <input
                type="text"
                value={addLabel}
                onChange={e => setAddLabel(e.target.value)}
                placeholder={t('proxies.labelPlaceholder')}
                className="flex-1 px-3 py-2 text-sm rounded-xl border border-border bg-background text-foreground placeholder:text-muted-foreground outline-none focus:ring-2 focus:ring-primary/30"
              />
              <button
                onClick={handleAdd}
                disabled={addLoading || !addInput.trim()}
                className="px-5 py-2 rounded-xl text-sm font-semibold bg-primary text-primary-foreground hover:opacity-90 transition-opacity disabled:opacity-50 shadow-sm"
              >
                {addLoading ? t('proxies.adding') : t('proxies.confirmAdd')}
              </button>
            </div>
          </CardContent>
        </Card>
      )}

      {/* Stats */}
      <div className="grid grid-cols-3 gap-4">
        <Card className="py-0">
          <CardContent className="p-4 text-center">
            <div className="text-2xl font-bold text-foreground">{proxies.length}</div>
            <div className="text-xs text-muted-foreground mt-1">{t('proxies.totalProxies')}</div>
          </CardContent>
        </Card>
        <Card className="py-0">
          <CardContent className="p-4 text-center">
            <div className="text-2xl font-bold text-emerald-600 dark:text-emerald-400">{enabledCount}</div>
            <div className="text-xs text-muted-foreground mt-1">{t('proxies.enabledCount')}</div>
          </CardContent>
        </Card>
        <Card className="py-0">
          <CardContent className="p-4 text-center">
            <div className={`text-2xl font-bold ${poolEnabled ? 'text-emerald-600 dark:text-emerald-400' : 'text-muted-foreground'}`}>
              {poolEnabled ? t('proxies.roundRobin') : t('proxies.off')}
            </div>
            <div className="text-xs text-muted-foreground mt-1">{t('proxies.poolStatus')}</div>
          </CardContent>
        </Card>
      </div>

      {/* Table */}
      <Card className="py-0">
        <CardContent className="p-0">
          {loading ? (
            <div className="flex justify-center items-center py-16">
              <Loader2 className="size-6 animate-spin text-primary" />
            </div>
          ) : proxies.length === 0 ? (
            <div className="text-center py-16 text-muted-foreground">
              <Globe className="size-12 mx-auto mb-3 opacity-30" />
              <p className="text-sm font-medium">{t('proxies.noProxies')}</p>
              <p className="text-xs mt-1">{t('proxies.noProxiesDesc')}</p>
            </div>
          ) : (
            <>
              <div className="overflow-x-auto">
                <table className="w-full text-sm">
                  <thead>
                    <tr className="border-b border-border text-left text-muted-foreground">
                      <th className="p-3 w-10">
                        <input type="checkbox" checked={allSelected} onChange={toggleSelectAll} className="size-4 rounded" />
                      </th>
                      <th className="p-3 font-semibold">{t('proxies.colUrl')}</th>
                      <th className="p-3 font-semibold">{t('proxies.colStatus')}</th>
                      <th className="p-3 font-semibold">{t('proxies.colLocation')}</th>
                      <th className="p-3 font-semibold">{t('proxies.colIp')}</th>
                      <th className="p-3 font-semibold">{t('proxies.colLatency')}</th>
                      <th className="p-3 font-semibold text-right">{t('proxies.colActions')}</th>
                    </tr>
                  </thead>
                  <tbody>
                    {pagedProxies.map(p => {
                      const isTesting = testingIds.has(p.id)
                      return (
                        <tr key={p.id} className="border-b border-border/50 hover:bg-muted/30 transition-colors">
                          <td className="p-3">
                            <input
                              type="checkbox"
                              checked={selected.has(p.id)}
                              onChange={() => {
                                const next = new Set(selected)
                                if (next.has(p.id)) next.delete(p.id)
                                else next.add(p.id)
                                setSelected(next)
                              }}
                              className="size-4 rounded"
                            />
                          </td>
                          <td className="p-3 max-w-[380px]">
                            <div className="flex items-center gap-2">
                              <button
                                onClick={() => {
                                  setRevealedIds(prev => {
                                    const next = new Set(prev)
                                    if (next.has(p.id)) next.delete(p.id)
                                    else next.add(p.id)
                                    return next
                                  })
                                }}
                                className="shrink-0 flex items-center justify-center size-6 rounded-md text-muted-foreground hover:text-foreground hover:bg-muted/50 transition-all"
                                title={revealedIds.has(p.id) ? 'Hide' : 'Show'}
                              >
                                {revealedIds.has(p.id) ? <EyeOff className="size-3.5" /> : <Eye className="size-3.5" />}
                              </button>
                              <span className="font-mono text-[20px] font-bold break-all text-foreground">
                                {revealedIds.has(p.id) ? p.url : maskUrl(p.url)}
                              </span>
                            </div>
                          </td>
                          <td className="p-3">
                            <button
                              onClick={() => handleToggle(p)}
                              className={`inline-flex items-center gap-1.5 px-2.5 py-1 rounded-full text-xs font-semibold transition-all ${
                                p.enabled
                                  ? 'bg-emerald-500/10 text-emerald-600 dark:text-emerald-400 border border-emerald-500/20'
                                  : 'bg-muted/50 text-muted-foreground border border-border'
                              }`}
                            >
                              <span className={`size-1.5 rounded-full ${p.enabled ? 'bg-emerald-500' : 'bg-muted-foreground/50'}`} />
                              {p.enabled ? t('proxies.enabled') : t('proxies.disabled')}
                            </button>
                          </td>
                          {/* Location */}
                          <td className="p-3">
                            {isTesting ? (
                              <Loader2 className="size-3.5 animate-spin text-muted-foreground" />
                            ) : p.test_location ? (
                              <div className="flex items-center gap-1 text-xs font-medium text-foreground whitespace-nowrap">
                                <MapPin className="size-3 text-primary shrink-0" />
                                {p.test_location}
                              </div>
                            ) : (
                              <span className="text-xs text-muted-foreground">-</span>
                            )}
                          </td>
                          {/* IP */}
                          <td className="p-3">
                            {p.test_ip ? (
                              <span className="text-[20px] font-mono font-bold text-foreground whitespace-nowrap">{p.test_ip}</span>
                            ) : (
                              <span className="text-xs text-muted-foreground">-</span>
                            )}
                          </td>
                          {/* Latency */}
                          <td className="p-3">
                            {p.test_latency_ms > 0 ? (
                              <span className={`inline-flex px-2 py-0.5 rounded-full text-xs font-bold ${latencyColor(p.test_latency_ms)} ${latencyBg(p.test_latency_ms)}`}>
                                {p.test_latency_ms}ms
                              </span>
                            ) : (
                              <span className="text-xs text-muted-foreground">-</span>
                            )}
                          </td>
                          <td className="p-3">
                            <div className="flex items-center gap-1.5 justify-end">
                              <button
                                onClick={() => handleTest(p)}
                                disabled={isTesting}
                                className="flex items-center gap-1 px-2.5 py-1.5 rounded-lg text-xs font-medium border border-border text-foreground hover:bg-muted/50 transition-all disabled:opacity-50"
                                title={t('proxies.testProxy')}
                              >
                                {isTesting ? <Loader2 className="size-3.5 animate-spin" /> : <Play className="size-3.5" />}
                                {t('proxies.test')}
                              </button>
                              <button
                                onClick={() => handleDelete(p.id)}
                                className="flex items-center justify-center size-7 rounded-lg text-destructive hover:bg-destructive/10 transition-all"
                                title={t('common.delete')}
                              >
                                <Trash2 className="size-3.5" />
                              </button>
                            </div>
                          </td>
                        </tr>
                      )
                    })}
                  </tbody>
                </table>
              </div>

              {/* Pagination */}
              {totalPages > 1 && (
                <div className="flex items-center justify-between px-4 py-3 border-t border-border">
                  <span className="text-xs text-muted-foreground">
                    {t('proxies.pagination', { total: proxies.length, page, totalPages })}
                  </span>
                  <div className="flex items-center gap-1">
                    <button
                      onClick={() => setPage(p => Math.max(1, p - 1))}
                      disabled={page <= 1}
                      className="flex items-center justify-center size-8 rounded-lg border border-border text-foreground hover:bg-muted/50 transition-all disabled:opacity-30 disabled:cursor-not-allowed"
                    >
                      <ChevronLeft className="size-4" />
                    </button>
                    {Array.from({ length: totalPages }, (_, i) => i + 1).map(n => (
                      <button
                        key={n}
                        onClick={() => setPage(n)}
                        className={`flex items-center justify-center size-8 rounded-lg text-xs font-medium transition-all ${
                          n === page
                            ? 'bg-primary text-primary-foreground shadow-sm'
                            : 'border border-border text-foreground hover:bg-muted/50'
                        }`}
                      >
                        {n}
                      </button>
                    ))}
                    <button
                      onClick={() => setPage(p => Math.min(totalPages, p + 1))}
                      disabled={page >= totalPages}
                      className="flex items-center justify-center size-8 rounded-lg border border-border text-foreground hover:bg-muted/50 transition-all disabled:opacity-30 disabled:cursor-not-allowed"
                    >
                      <ChevronRight className="size-4" />
                    </button>
                  </div>
                </div>
              )}
            </>
          )}
        </CardContent>
      </Card>
    </div>
  )
}
