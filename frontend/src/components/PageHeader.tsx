import type { ReactNode } from 'react'
import { Button } from '@/components/ui/button'
import { RefreshCw } from 'lucide-react'
import { useTranslation } from 'react-i18next'

interface PageHeaderProps {
  title: string
  description?: string
  onRefresh?: () => void
  refreshLabel?: string
  actions?: ReactNode
}

export default function PageHeader({
  title,
  description,
  onRefresh,
  refreshLabel,
  actions,
}: PageHeaderProps) {
  const { t } = useTranslation()
  const hasActions = Boolean(onRefresh) || Boolean(actions)
  const resolvedRefreshLabel = refreshLabel ?? t('common.refresh')

  return (
    <div className="flex items-end justify-between gap-6 mb-8 max-sm:flex-col max-sm:items-stretch">
      <div className="max-w-[760px]">
        <h2 className="text-[clamp(32px,4vw,42px)] font-semibold leading-[1.08] tracking-tight">
          {title}
        </h2>
        {description ? (
          <p className="mt-3 max-w-[640px] text-muted-foreground text-[15px] leading-relaxed">
            {description}
          </p>
        ) : null}
      </div>
      {hasActions ? (
        <div className="flex gap-3 items-center max-sm:w-full">
          {onRefresh ? (
            <Button variant="outline" onClick={onRefresh} className="max-sm:w-full">
              <RefreshCw className="size-3.5" />
              {resolvedRefreshLabel}
            </Button>
          ) : null}
          {actions}
        </div>
      ) : null}
    </div>
  )
}
