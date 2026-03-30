import { useCallback, useEffect, useMemo, useState } from 'react'
import { useTranslation } from 'react-i18next'
import {
  CartesianGrid,
  Legend,
  Line,
  LineChart,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from 'recharts'
import { api } from '../api'
import { Card, CardContent } from '@/components/ui/card'
import type { AccountEventTrendPoint } from '../types'

type TrendRange = '24h' | '7d' | '30d'

const RANGE_OPTIONS: TrendRange[] = ['24h', '7d', '30d']

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

function getRangeConfig(range: TrendRange) {
  switch (range) {
    case '24h': return { bucketMinutes: 60, offsetMs: 24 * 3600_000 }
    case '7d':  return { bucketMinutes: 360, offsetMs: 7 * 86400_000 }
    case '30d': return { bucketMinutes: 1440, offsetMs: 30 * 86400_000 }
  }
}

function toRFC3339(date: Date): string {
  const pad = (n: number) => String(n).padStart(2, '0')
  const off = date.getTimezoneOffset()
  const sign = off <= 0 ? '+' : '-'
  const abs = Math.abs(off)
  return `${date.getFullYear()}-${pad(date.getMonth() + 1)}-${pad(date.getDate())}T${pad(date.getHours())}:${pad(date.getMinutes())}:${pad(date.getSeconds())}${sign}${pad(Math.floor(abs / 60))}:${pad(abs % 60)}`
}

interface DisplayPoint {
  label: string
  fullLabel: string
  added: number
  deleted: number
}

export default function AccountTrendChart() {
  const { t } = useTranslation()
  const [range, setRange] = useState<TrendRange>('7d')
  const [rawData, setRawData] = useState<AccountEventTrendPoint[]>([])
  const [loading, setLoading] = useState(false)

  const fetchData = useCallback(async () => {
    setLoading(true)
    try {
      const cfg = getRangeConfig(range)
      const now = new Date()
      const start = toRFC3339(new Date(now.getTime() - cfg.offsetMs))
      const end = toRFC3339(now)
      const resp = await api.getAccountEventTrend({ start, end, bucketMinutes: cfg.bucketMinutes })
      setRawData(resp.trend ?? [])
    } catch (err) {
      console.error('Failed to load account event trend:', err)
    } finally {
      setLoading(false)
    }
  }, [range])

  useEffect(() => { void fetchData() }, [fetchData])

  // 15 秒自动刷新，与运维页一致
  useEffect(() => {
    const timer = setInterval(() => { void fetchData() }, 15_000)
    return () => clearInterval(timer)
  }, [fetchData])

  const { bucketMinutes } = getRangeConfig(range)

  const displayData = useMemo<DisplayPoint[]>(() => {
    return rawData.map((p) => {
      const d = new Date(p.bucket)
      const mm = String(d.getMonth() + 1).padStart(2, '0')
      const dd = String(d.getDate()).padStart(2, '0')
      const hh = String(d.getHours()).padStart(2, '0')
      const mi = String(d.getMinutes()).padStart(2, '0')

      let label: string
      let fullLabel: string
      if (bucketMinutes >= 1440) {
        label = `${mm}-${dd}`
        fullLabel = `${d.getFullYear()}-${mm}-${dd}`
      } else if (bucketMinutes >= 360) {
        label = `${mm}-${dd} ${hh}:00`
        fullLabel = `${mm}-${dd} ${hh}:${mi}`
      } else {
        label = `${hh}:${mi}`
        fullLabel = `${mm}-${dd} ${hh}:${mi}`
      }

      return { label, fullLabel, added: p.added, deleted: p.deleted }
    })
  }, [rawData, bucketMinutes])

  const totalAdded = displayData.reduce((s, p) => s + p.added, 0)
  const totalDeleted = displayData.reduce((s, p) => s + p.deleted, 0)

  return (
    <Card className="py-0">
      <CardContent className="p-6">
        {/* 标题 + 时间范围切换 */}
        <div className="flex items-start justify-between gap-4 flex-wrap mb-5">
          <div>
            <h3 className="text-base font-semibold text-foreground">
              {t('ops.accountTrendTitle')}
            </h3>
            <p className="mt-1 text-sm text-muted-foreground">
              {t('ops.accountTrendDesc', { added: totalAdded, deleted: totalDeleted })}
            </p>
          </div>
          <div className="inline-flex rounded-lg border border-border bg-muted/50 p-0.5">
            {RANGE_OPTIONS.map((key) => (
              <button
                key={key}
                type="button"
                onClick={() => setRange(key)}
                className={`px-3 py-1.5 text-xs font-medium rounded-md transition-all duration-200 ${
                  range === key
                    ? 'bg-background text-foreground shadow-sm border border-border'
                    : 'text-muted-foreground hover:text-foreground'
                }`}
              >
                {key}
              </button>
            ))}
          </div>
        </div>

        {/* 图表区域 */}
        <div className="h-[320px]">
          {loading && displayData.length === 0 ? (
            <div className="flex items-center justify-center h-full text-muted-foreground text-sm">
              {t('common.loading')}
            </div>
          ) : displayData.length === 0 ? (
            <div className="flex items-center justify-center h-full text-muted-foreground text-sm">
              {t('ops.accountTrendEmpty')}
            </div>
          ) : (
            <ResponsiveContainer width="100%" height="100%">
              <LineChart data={displayData} margin={chartMargin}>
                <CartesianGrid vertical={false} stroke={gridColor} strokeDasharray="4 4" />
                <XAxis
                  dataKey="label"
                  tick={{ fill: axisColor, fontSize: 12 }}
                  axisLine={{ stroke: gridColor }}
                  tickLine={{ stroke: gridColor }}
                  minTickGap={20}
                  tickMargin={8}
                />
                <YAxis
                  tick={{ fill: axisColor, fontSize: 12 }}
                  axisLine={{ stroke: gridColor }}
                  tickLine={{ stroke: gridColor }}
                  allowDecimals={false}
                />
                <Tooltip
                  labelFormatter={(_, payload) => {
                    const p = payload?.[0]?.payload as DisplayPoint | undefined
                    return p?.fullLabel ?? ''
                  }}
                  contentStyle={tooltipContentStyle}
                  labelStyle={tooltipLabelStyle}
                  itemStyle={tooltipItemStyle}
                />
                <Legend wrapperStyle={{ paddingTop: 12, fontSize: 12, color: axisColor }} />
                <Line
                  type="monotone"
                  dataKey="added"
                  name={t('ops.accountTrendAdded')}
                  stroke="hsl(var(--success))"
                  strokeWidth={2.5}
                  dot={{ r: 3, fill: 'hsl(var(--success))' }}
                  activeDot={{ r: 5 }}
                />
                <Line
                  type="monotone"
                  dataKey="deleted"
                  name={t('ops.accountTrendDeleted')}
                  stroke="var(--color-destructive)"
                  strokeWidth={2.5}
                  dot={{ r: 3, fill: 'var(--color-destructive)' }}
                  activeDot={{ r: 5 }}
                />
              </LineChart>
            </ResponsiveContainer>
          )}
        </div>
      </CardContent>
    </Card>
  )
}
