import type { ReactNode } from 'react'
import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
  DialogFooter,
} from '@/components/ui/dialog'
import { cn } from '@/lib/utils'

interface ModalProps {
  show: boolean
  title: string
  onClose: () => void
  children: ReactNode
  footer?: ReactNode
  contentClassName?: string
  bodyClassName?: string
  titleClassName?: string
  showCloseButton?: boolean
}

export default function Modal({
  show,
  title,
  onClose,
  children,
  footer,
  contentClassName,
  bodyClassName,
  titleClassName,
  showCloseButton = true,
}: ModalProps) {
  return (
    <Dialog open={show} onOpenChange={(open) => { if (!open) onClose() }}>
      <DialogContent
        showCloseButton={showCloseButton}
        className={cn(
          'max-h-[calc(100vh-2rem)] overflow-hidden p-0 sm:max-w-[520px]',
          contentClassName
        )}
      >
        <div className="flex max-h-[calc(100vh-2rem)] min-w-0 flex-col">
          <DialogHeader className="min-w-0 shrink-0 border-b px-6 pt-6 pb-4 pr-12">
            <DialogTitle className={cn('min-w-0 text-xl leading-snug break-all', titleClassName)}>
              {title}
            </DialogTitle>
          </DialogHeader>
          <div className={cn('min-h-0 flex-1 overflow-y-auto px-6 py-4', bodyClassName)}>{children}</div>
          {footer ? <DialogFooter className="shrink-0 border-t px-6 py-4">{footer}</DialogFooter> : null}
        </div>
      </DialogContent>
    </Dialog>
  )
}
