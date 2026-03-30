import type { ReactNode } from 'react'
import { useCallback, useEffect, useRef, useState } from 'react'
import { useTranslation } from 'react-i18next'
import { api } from '../api'
import DashboardUsageCharts, { getTimeRangeISO, getBucketConfig } from '../components/DashboardUsageCharts'
import type { TimeRangeKey } from '../components/DashboardUsageCharts'
import PageHeader from '../components/PageHeader'
import StateShell from '../components/StateShell'
import StatCard from '../components/StatCard'
import type { StatsResponse, UsageStats, ChartAggregation } from '../types'
import { useDataLoader } from '../hooks/useDataLoader'
import { Card, CardContent } from '@/components/ui/card'
import { Users, CheckCircle, XCircle, Activity, Zap, Clock, AlertTriangle, BarChart3, Database } from 'lucide-react'

const DASHBOARD_REFRESH_INTERVAL_MS = 15_000

export default function Dashboard() {
  const { t } = useTranslation()
  const [timeRange, setTimeRange] = useState<TimeRangeKey>('1h')
  const [chartData, setChartData] = useState<ChartAggregation | null>(null)
  const [chartRefreshedAt, setChartRefreshedAt] = useState<number | null>(null)
  const [chartLoading, setChartLoading] = useState(true)
  const chartAbort = useRef<AbortController | null>(null)

  // 仅加载轻量级统计数据（秒级响应）
  const loadDashboardStats = useCallback(async () => {
    const [stats, usageStats] = await Promise.all([
      api.getStats(),
      api.getUsageStats(),
    ])
    return { stats, usageStats }
  }, [])

  const { data, loading, error, reload, reloadSilently } = useDataLoader<{
    stats: StatsResponse | null
    usageStats: UsageStats | null
  }>({
    initialData: { stats: null, usageStats: null },
    load: loadDashboardStats,
  })

  // 加载服务端聚合的图表数据（12~48 个聚合点，非原始行）
  const loadChartData = useCallback(async () => {
    chartAbort.current?.abort()
    const controller = new AbortController()
    chartAbort.current = controller
    setChartLoading(true)
    try {
      const { start, end } = getTimeRangeISO(timeRange)
      const { bucketMinutes } = getBucketConfig(timeRange)
      const res = await api.getChartData({ start, end, bucketMinutes })
      if (!controller.signal.aborted) {
        setChartData(res)
        setChartRefreshedAt(Date.now())
      }
    } catch {
      // 静默容错
    } finally {
      if (!controller.signal.aborted) {
        setChartLoading(false)
      }
    }
  }, [timeRange])

  // 首次加载 + timeRange 变更时重新拉取图表数据
  useEffect(() => {
    void loadChartData()
  }, [loadChartData])

  // 仅在 1h（实时）模式下启用自动刷新
  useEffect(() => {
    if (timeRange !== '1h') return

    const timer = window.setInterval(() => {
      if (document.visibilityState !== 'visible') return
      void reloadSilently()
      void loadChartData()
    }, DASHBOARD_REFRESH_INTERVAL_MS)

    return () => window.clearInterval(timer)
  }, [reloadSilently, timeRange, loadChartData])

  const { stats, usageStats } = data
  const total = stats?.total ?? 0
  const available = stats?.available ?? 0
  const errorCount = stats?.error ?? 0
  const todayRequests = stats?.today_requests ?? 0

  const icons: Record<string, ReactNode> = {
    total: <Users className="size-[22px]" />,
    available: <CheckCircle className="size-[22px]" />,
    error: <XCircle className="size-[22px]" />,
    requests: <Activity className="size-[22px]" />,
  }

  return (
    <StateShell
      variant="page"
      loading={loading}
      error={error}
      onRetry={() => { void reload(); void loadChartData() }}
      loadingTitle={t('dashboard.loadingTitle')}
      loadingDescription={t('dashboard.loadingDesc')}
      errorTitle={t('dashboard.errorTitle')}
    >
      <>
        <PageHeader
          title={t('dashboard.title')}
          description={t('dashboard.description')}
          onRefresh={() => { void reload(); void loadChartData() }}
        />

        {/* Account status */}
        <div className="grid grid-cols-[repeat(auto-fit,minmax(220px,1fr))] gap-4 mb-6">
          <StatCard icon={icons.total} iconClass="blue" label={t('dashboard.totalAccounts')} value={total} />
          <StatCard
            icon={icons.available}
            iconClass="green"
            label={t('dashboard.available')}
            value={available}
            sub={t('dashboard.availableRate', { rate: total ? Math.round((available / total) * 100) : 0 })}
          />
          <StatCard icon={icons.error} iconClass="red" label={t('dashboard.error')} value={errorCount} />
          <StatCard icon={icons.requests} iconClass="purple" label={t('dashboard.todayRequests')} value={todayRequests} />
        </div>

        {/* Usage stats */}
        {usageStats && (
          <div className="space-y-6">
            <Card>
              <CardContent className="p-6">
                <h3 className="text-base font-semibold text-foreground mb-4">{t('dashboard.usageStats')}</h3>
                <div className="grid grid-cols-[repeat(auto-fit,minmax(200px,1fr))] gap-4">
                  <StatItem icon={<BarChart3 className="size-5" />} iconBg="bg-blue-500/10 text-blue-500" label={t('dashboard.totalRequests')} value={usageStats.total_requests.toLocaleString()} />
                  <StatItem icon={<Zap className="size-5" />} iconBg="bg-purple-500/10 text-purple-500" label={t('dashboard.totalTokens')} value={usageStats.total_tokens.toLocaleString()} />
                  <StatItem icon={<Zap className="size-5" />} iconBg="bg-emerald-500/10 text-emerald-500" label={t('dashboard.todayTokens')} value={usageStats.today_tokens.toLocaleString()} />
                  <StatItem icon={<Database className="size-5" />} iconBg="bg-indigo-500/10 text-indigo-500" label={t('dashboard.cachedTokens')} value={usageStats.total_cached_tokens.toLocaleString()} />
                  <StatItem icon={<Activity className="size-5" />} iconBg="bg-amber-500/10 text-amber-500" label={t('dashboard.rpmTpm')} value={`${usageStats.rpm} / ${usageStats.tpm.toLocaleString()}`} />
                  <StatItem
                    icon={<Clock className="size-5" />}
                    iconBg="bg-cyan-500/10 text-cyan-500"
                    label={t('dashboard.avgLatency')}
                    value={usageStats.avg_duration_ms > 1000 ? `${(usageStats.avg_duration_ms / 1000).toFixed(1)}s` : `${Math.round(usageStats.avg_duration_ms)}ms`}
                  />
                  <StatItem icon={<AlertTriangle className="size-5" />} iconBg="bg-red-500/10 text-red-500" label={t('dashboard.todayErrorRate')} value={`${usageStats.error_rate.toFixed(1)}%`} />
                </div>
              </CardContent>
            </Card>
            <DashboardUsageCharts
              chartData={chartData}
              refreshedAt={chartRefreshedAt}
              refreshIntervalMs={DASHBOARD_REFRESH_INTERVAL_MS}
              timeRange={timeRange}
              onTimeRangeChange={setTimeRange}
              loading={chartLoading}
            />
          </div>
        )}
      </>
    </StateShell>
  )
}

function StatItem({ icon, iconBg, label, value }: { icon: ReactNode; iconBg: string; label: string; value: string }) {
  return (
    <div className="flex items-center gap-3 p-4 rounded-xl bg-muted/50">
      <div className={`flex items-center justify-center size-10 rounded-lg ${iconBg}`}>
        {icon}
      </div>
      <div>
        <div className="text-xs text-muted-foreground">{label}</div>
        <div className="text-lg font-bold">{value}</div>
      </div>
    </div>
  )
}
