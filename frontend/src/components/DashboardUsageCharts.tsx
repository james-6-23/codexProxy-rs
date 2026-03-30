import type { ReactNode } from 'react'
import { useMemo } from 'react'
import { useTranslation } from 'react-i18next'
import {
  Area,
  Bar,
  BarChart,
  CartesianGrid,
  ComposedChart,
  Legend,
  Line,
  LineChart,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from 'recharts'
import { Card, CardContent } from '@/components/ui/card'
import StateShell from './StateShell'
import type { ChartAggregation } from '../types'

export type TimeRangeKey = '1h' | '6h' | '24h' | '7d' | '30d'

interface DashboardUsageChartsProps {
  chartData: ChartAggregation | null
  refreshedAt: number | null
  refreshIntervalMs: number
  timeRange: TimeRangeKey
  onTimeRangeChange: (range: TimeRangeKey) => void
  loading?: boolean
}

interface TimelinePoint {
  label: string
  fullLabel: string
  requests: number
  avgLatency: number | null
  inputTokens: number
  outputTokens: number
  reasoningTokens: number
  cachedTokens: number
  errors401: number
}

interface ModelRankingPoint {
  model: string
  shortModel: string
  requests: number
}

const chartMargin = { top: 8, right: 12, left: -12, bottom: 0 }
const gridColor = 'var(--color-border)'
const axisColor = 'var(--color-muted-foreground)'
const tooltipContentStyle = {
  backgroundColor: 'var(--color-card)',
  border: '1px solid var(--color-border)',
  borderRadius: '16px',
  boxShadow: '0 18px 40px rgba(0, 0, 0, 0.12)',
}
const tooltipLabelStyle = { color: 'var(--color-foreground)', fontWeight: 600 }
const tooltipItemStyle = { color: 'var(--color-foreground)' }
const tokenBreakdownTooltipPosition = { y: -160 }
const compactNumberFormatter = new Intl.NumberFormat(undefined, {
  notation: 'compact',
  maximumFractionDigits: 1,
})

const TIME_RANGE_OPTIONS: TimeRangeKey[] = ['1h', '6h', '24h', '7d', '30d']

/** 根据时间跨度计算桶大小（分钟）和桶数量 */
export function getBucketConfig(range: TimeRangeKey): { bucketMinutes: number; bucketCount: number } {
  switch (range) {
    case '1h':
      return { bucketMinutes: 5, bucketCount: 12 }
    case '6h':
      return { bucketMinutes: 15, bucketCount: 24 }
    case '24h':
      return { bucketMinutes: 30, bucketCount: 48 }
    case '7d':
      return { bucketMinutes: 360, bucketCount: 28 }
    case '30d':
      return { bucketMinutes: 1440, bucketCount: 30 }
    default:
      return { bucketMinutes: 5, bucketCount: 12 }
  }
}

export default function DashboardUsageCharts({
  chartData: serverData,
  refreshedAt,
  refreshIntervalMs,
  timeRange,
  onTimeRangeChange,
  loading = false,
}: DashboardUsageChartsProps) {
  const { t } = useTranslation()
  const { bucketMinutes, bucketCount } = getBucketConfig(timeRange)
  const isLive = timeRange === '1h'
  const lastUpdatedAtLabel = formatClockTime(refreshedAt)
  const useFullDate = bucketMinutes >= 360

  // 将服务端聚合数据映射为图表渲染格式（极轻量，无聚合计算）
  const displayData = useMemo(() => {
    if (!serverData) return { timelineData: [] as TimelinePoint[], modelData: [] as ModelRankingPoint[], sampleCount: 0 }

    const totalRequests = serverData.timeline.reduce((sum, p) => sum + p.requests, 0)

    const timelineData: TimelinePoint[] = serverData.timeline.map((point) => {
      const d = new Date(point.bucket)
      return {
        label: useFullDate ? formatDateLabel(d, bucketMinutes) : formatMinuteLabel(d),
        fullLabel: formatFullLabel(d, bucketMinutes),
        requests: point.requests,
        avgLatency: point.avg_latency > 0 ? Math.round(point.avg_latency) : null,
        inputTokens: point.input_tokens,
        outputTokens: point.output_tokens,
        reasoningTokens: point.reasoning_tokens,
        cachedTokens: point.cached_tokens,
        errors401: point.errors_401,
      }
    })

    const modelData: ModelRankingPoint[] = serverData.models
      .slice()
      .sort((a, b) => b.requests - a.requests)
      .slice(0, 5)
      .map((m) => ({
        model: m.model,
        shortModel: truncateLabel(m.model, 22),
        requests: m.requests,
      }))

    return { timelineData, modelData, sampleCount: totalRequests }
  }, [serverData, useFullDate, bucketMinutes])

  return (
    <div className="space-y-4">
      <div className="flex items-start justify-between gap-4 flex-wrap">
        <div>
          <h3 className="text-base font-semibold text-foreground">{t('dashboard.usageCharts')}</h3>
          <p className="mt-1 text-sm text-muted-foreground">{t('dashboard.usageChartsDesc', { count: displayData.sampleCount.toLocaleString() })}</p>
          {isLive && (
            <p className="mt-1 text-xs text-muted-foreground">
              {t('dashboard.liveWindowDesc', {
                hours: (bucketCount * bucketMinutes) / 60,
                minutes: bucketMinutes,
                seconds: Math.round(refreshIntervalMs / 1000),
                time: lastUpdatedAtLabel,
              })}
            </p>
          )}
        </div>
        <div className="flex items-center gap-2">
          {isLive && (
            <div className="inline-flex items-center gap-2 rounded-full border border-emerald-500/20 bg-emerald-500/10 px-3 py-1 text-xs font-medium text-emerald-600 dark:text-emerald-300 mr-2">
              <span className="size-2 rounded-full bg-current animate-pulse" />
              <span>{t('dashboard.liveBadge')}</span>
            </div>
          )}
          <div className="inline-flex rounded-lg border border-border bg-muted/50 p-0.5">
            {TIME_RANGE_OPTIONS.map((key) => (
              <button
                key={key}
                type="button"
                onClick={() => onTimeRangeChange(key)}
                className={`px-3 py-1.5 text-xs font-medium rounded-md transition-all duration-200 ${
                  timeRange === key
                    ? 'bg-background text-foreground shadow-sm border border-border'
                    : 'text-muted-foreground hover:text-foreground'
                }`}
              >
                {t(`dashboard.timeRange${key.toUpperCase()}`)}
              </button>
            ))}
          </div>
        </div>
      </div>

      {loading ? (
        <div className="grid grid-cols-1 gap-4 xl:grid-cols-2">
          {[0, 1, 2, 3].map((i) => (
            <Card key={i} className="py-0">
              <CardContent className="p-6">
                <div className="mb-5 space-y-2">
                  <div className="h-4 w-32 rounded-md bg-muted animate-pulse" />
                  <div className="h-3 w-48 rounded-md bg-muted/60 animate-pulse" />
                </div>
                <div className="h-[280px] flex items-end gap-2 px-4 pb-4">
                  {[40, 65, 30, 80, 55, 70, 45, 60, 35, 75, 50, 68].map((h, j) => (
                    <div
                      key={j}
                      className="flex-1 rounded-t-md bg-muted/50 animate-pulse"
                      style={{ height: `${h}%`, animationDelay: `${j * 80}ms` }}
                    />
                  ))}
                </div>
              </CardContent>
            </Card>
          ))}
        </div>
      ) : displayData.sampleCount === 0 ? (
        <Card>
          <CardContent className="p-6">
            <StateShell
              variant="section"
              isEmpty
              emptyTitle={t('dashboard.chartsEmptyTitle')}
              emptyDescription={t('dashboard.chartsEmptyDesc')}
            >
              <></>
            </StateShell>
          </CardContent>
        </Card>
      ) : (
        <div className="grid grid-cols-1 gap-4 xl:grid-cols-2">
          <ChartCard title={t('dashboard.requestTrend')} description={t('dashboard.requestTrendDesc')}>
            <ResponsiveContainer width="100%" height="100%">
              <ComposedChart data={displayData.timelineData} margin={chartMargin}>
                <defs>
                  <linearGradient id="dashboard-request-gradient" x1="0" y1="0" x2="0" y2="1">
                    <stop offset="5%" stopColor="var(--color-primary)" stopOpacity={0.28} />
                    <stop offset="95%" stopColor="var(--color-primary)" stopOpacity={0} />
                  </linearGradient>
                </defs>
                <CartesianGrid vertical={false} stroke={gridColor} strokeDasharray="4 4" />
                <XAxis dataKey="label" tick={{ fill: axisColor, fontSize: 12 }} axisLine={{ stroke: gridColor }} tickLine={{ stroke: gridColor }} minTickGap={20} tickMargin={8} />
                <YAxis tickFormatter={formatCompactNumber} tick={{ fill: axisColor, fontSize: 12 }} axisLine={{ stroke: gridColor }} tickLine={{ stroke: gridColor }} allowDecimals={false} tickCount={8} />
                <Tooltip
                  position={{ y: 10 }}
                  formatter={(value) => formatNumber(value)}
                  labelFormatter={(_, payload) => getTooltipLabel(payload, 'fullLabel')}
                  contentStyle={tooltipContentStyle}
                  labelStyle={tooltipLabelStyle}
                  itemStyle={tooltipItemStyle}
                  itemSorter={(item) => (item.dataKey === 'requests' ? -1 : 1)}
                />
                <Legend wrapperStyle={{ paddingTop: 4, fontSize: 12 }} />
                <Area
                  type="monotone"
                  dataKey="requests"
                  name={t('dashboard.seriesRequests')}
                  stroke="var(--color-primary)"
                  fill="url(#dashboard-request-gradient)"
                  strokeWidth={2.5}
                />
                <Line
                  type="monotone"
                  dataKey="errors401"
                  name={t('dashboard.series401Errors')}
                  stroke="var(--color-destructive)"
                  strokeWidth={2}
                  dot={false}
                  activeDot={{ r: 4 }}
                />
              </ComposedChart>
            </ResponsiveContainer>
          </ChartCard>

          <ChartCard title={t('dashboard.latencyTrend')} description={t('dashboard.latencyTrendDesc')}>
            <ResponsiveContainer width="100%" height="100%">
              <LineChart data={displayData.timelineData} margin={chartMargin}>
                <CartesianGrid vertical={false} stroke={gridColor} strokeDasharray="4 4" />
                <XAxis dataKey="label" tick={{ fill: axisColor, fontSize: 12 }} axisLine={{ stroke: gridColor }} tickLine={{ stroke: gridColor }} minTickGap={20} tickMargin={8} />
                <YAxis tickFormatter={formatDurationTick} tick={{ fill: axisColor, fontSize: 12 }} axisLine={{ stroke: gridColor }} tickLine={{ stroke: gridColor }} width={54} />
                <Tooltip
                  position={{ y: 10 }}
                  formatter={(value) => formatDuration(value)}
                  labelFormatter={(_, payload) => getTooltipLabel(payload, 'fullLabel')}
                  contentStyle={tooltipContentStyle}
                  labelStyle={tooltipLabelStyle}
                  itemStyle={tooltipItemStyle}
                />
                <Line
                  type="monotone"
                  dataKey="avgLatency"
                  name={t('dashboard.seriesAvgLatency')}
                  stroke="hsl(var(--info))"
                  strokeWidth={2.5}
                  dot={false}
                  connectNulls
                  activeDot={{ r: 4 }}
                />
              </LineChart>
            </ResponsiveContainer>
          </ChartCard>

          <ChartCard title={t('dashboard.tokenBreakdown')} description={t('dashboard.tokenBreakdownDesc')}>
            <ResponsiveContainer width="100%" height="100%">
              <BarChart data={displayData.timelineData} margin={chartMargin}>
                <CartesianGrid vertical={false} stroke={gridColor} strokeDasharray="4 4" />
                <XAxis dataKey="label" tick={{ fill: axisColor, fontSize: 12 }} axisLine={{ stroke: gridColor }} tickLine={{ stroke: gridColor }} minTickGap={20} tickMargin={8} />
                <YAxis tickFormatter={formatCompactNumber} tick={{ fill: axisColor, fontSize: 12 }} axisLine={{ stroke: gridColor }} tickLine={{ stroke: gridColor }} />
                <Tooltip
                  position={tokenBreakdownTooltipPosition}
                  allowEscapeViewBox={{ y: true }}
                  offset={20}
                  formatter={(value) => formatNumber(value)}
                  labelFormatter={(_, payload) => getTooltipLabel(payload, 'fullLabel')}
                  contentStyle={tooltipContentStyle}
                  labelStyle={tooltipLabelStyle}
                  itemStyle={tooltipItemStyle}
                />
                <Legend wrapperStyle={{ paddingTop: 12, fontSize: 12, color: axisColor }} />
                <Bar dataKey="inputTokens" stackId="tokens" name={t('dashboard.seriesInputTokens')} fill="hsl(var(--info))" radius={[0, 0, 4, 4]} />
                <Bar dataKey="outputTokens" stackId="tokens" name={t('dashboard.seriesOutputTokens')} fill="hsl(var(--success))" minPointSize={4} />
                <Bar dataKey="reasoningTokens" stackId="tokens" name={t('dashboard.seriesReasoningTokens')} fill="hsl(36 90% 55%)" minPointSize={4} />
                <Bar dataKey="cachedTokens" stackId="tokens" name={t('dashboard.seriesCachedTokens')} fill="hsl(262 83% 58%)" radius={[4, 4, 0, 0]} />
              </BarChart>
            </ResponsiveContainer>
          </ChartCard>

          <ChartCard title={t('dashboard.modelRanking')} description={t('dashboard.modelRankingDesc')}>
            <ResponsiveContainer width="100%" height="100%">
              <BarChart data={displayData.modelData} layout="vertical" margin={{ top: 8, right: 12, left: 8, bottom: 0 }}>
                <CartesianGrid horizontal={false} stroke={gridColor} strokeDasharray="4 4" />
                <XAxis type="number" tickFormatter={formatCompactNumber} tick={{ fill: axisColor, fontSize: 12 }} axisLine={{ stroke: gridColor }} tickLine={{ stroke: gridColor }} allowDecimals={false} />
                <YAxis dataKey="shortModel" type="category" width={128} tick={{ fill: axisColor, fontSize: 12 }} axisLine={{ stroke: gridColor }} tickLine={{ stroke: gridColor }} />
                <Tooltip
                  position={{ y: 10 }}
                  formatter={(value) => formatNumber(value)}
                  labelFormatter={(_, payload) => getTooltipLabel(payload, 'model')}
                  contentStyle={tooltipContentStyle}
                  labelStyle={tooltipLabelStyle}
                  itemStyle={tooltipItemStyle}
                />
                <Bar dataKey="requests" name={t('dashboard.seriesRequestCount')} fill="hsl(var(--success))" radius={[0, 8, 8, 0]} />
              </BarChart>
            </ResponsiveContainer>
          </ChartCard>
        </div>
      )}
    </div>
  )
}

function ChartCard({ title, description, children }: { title: string; description: string; children: ReactNode }) {
  return (
    <Card className="py-0">
      <CardContent className="p-6">
        <div className="mb-5">
          <h4 className="text-base font-semibold text-foreground">{title}</h4>
          <p className="mt-1 text-sm leading-relaxed text-muted-foreground">{description}</p>
        </div>
        <div className="h-[280px]">{children}</div>
      </CardContent>
    </Card>
  )
}

function formatMinuteLabel(date: Date): string {
  const hours = String(date.getHours()).padStart(2, '0')
  const minutes = String(date.getMinutes()).padStart(2, '0')
  return `${hours}:${minutes}`
}

function formatDateLabel(date: Date, bucketMinutes: number): string {
  const month = String(date.getMonth() + 1).padStart(2, '0')
  const day = String(date.getDate()).padStart(2, '0')
  if (bucketMinutes >= 1440) {
    return `${month}-${day}`
  }
  const hour = String(date.getHours()).padStart(2, '0')
  return `${month}-${day} ${hour}:00`
}

function formatFullLabel(date: Date, bucketMinutes: number): string {
  const month = String(date.getMonth() + 1).padStart(2, '0')
  const day = String(date.getDate()).padStart(2, '0')
  const hour = String(date.getHours()).padStart(2, '0')
  const minute = String(date.getMinutes()).padStart(2, '0')
  if (bucketMinutes >= 1440) {
    return `${date.getFullYear()}-${month}-${day}`
  }
  return `${month}-${day} ${hour}:${minute}`
}

function formatClockTime(value: number | null): string {
  if (!value) return '--:--:--'
  const date = new Date(value)
  if (Number.isNaN(date.getTime())) return '--:--:--'
  return date.toLocaleTimeString([], {
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
  })
}

function truncateLabel(value: string, maxLength: number): string {
  if (value.length <= maxLength) return value
  return `${value.slice(0, maxLength - 1)}…`
}

function formatCompactNumber(value: number | string): string {
  const numericValue = typeof value === 'number' ? value : Number(value)
  if (!Number.isFinite(numericValue)) return '0'
  return compactNumberFormatter.format(numericValue)
}

function formatNumber(value: unknown): string {
  const numericValue = typeof value === 'number' ? value : Number(value)
  if (!Number.isFinite(numericValue)) return '0'
  return numericValue.toLocaleString()
}

function formatDuration(value: unknown): string {
  const numericValue = typeof value === 'number' ? value : Number(value)
  if (!Number.isFinite(numericValue) || numericValue <= 0) return '-'
  if (numericValue >= 1000) {
    return `${(numericValue / 1000).toFixed(numericValue >= 10000 ? 0 : 1)}s`
  }
  return `${Math.round(numericValue)}ms`
}

function formatDurationTick(value: number | string): string {
  const numericValue = typeof value === 'number' ? value : Number(value)
  if (!Number.isFinite(numericValue)) return '0ms'
  return formatDuration(numericValue)
}

function getTooltipLabel(payload: readonly { payload?: Record<string, unknown> }[] | undefined, key: string): string {
  const tooltipPayload = payload?.[0]?.payload
  const rawValue = tooltipPayload?.[key]
  return typeof rawValue === 'string' && rawValue ? rawValue : ''
}

/** 将 Date 格式化为带本地时区偏移的 RFC3339 字符串（避免 UTC/本地时间不一致） */
function toLocalRFC3339(date: Date): string {
  const pad = (n: number) => String(n).padStart(2, '0')
  const offset = date.getTimezoneOffset()
  const sign = offset <= 0 ? '+' : '-'
  const absOffset = Math.abs(offset)
  const tzH = pad(Math.floor(absOffset / 60))
  const tzM = pad(absOffset % 60)
  return `${date.getFullYear()}-${pad(date.getMonth() + 1)}-${pad(date.getDate())}T${pad(date.getHours())}:${pad(date.getMinutes())}:${pad(date.getSeconds())}${sign}${tzH}:${tzM}`
}

/** 根据 TimeRangeKey 计算时间范围的起始 ISO 字符串 */
export function getTimeRangeISO(range: TimeRangeKey): { start: string; end: string } {
  const now = new Date()
  const end = toLocalRFC3339(now)
  let offsetMs: number
  switch (range) {
    case '1h':
      offsetMs = 60 * 60 * 1000
      break
    case '6h':
      offsetMs = 6 * 60 * 60 * 1000
      break
    case '24h':
      offsetMs = 24 * 60 * 60 * 1000
      break
    case '7d':
      offsetMs = 7 * 24 * 60 * 60 * 1000
      break
    case '30d':
      offsetMs = 30 * 24 * 60 * 60 * 1000
      break
    default:
      offsetMs = 60 * 60 * 1000
  }
  const start = toLocalRFC3339(new Date(now.getTime() - offsetMs))
  return { start, end }
}
