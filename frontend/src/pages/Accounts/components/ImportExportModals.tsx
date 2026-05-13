import { useTranslation } from 'react-i18next'
import Modal from '../../../components/Modal'
import { FileText, FileJson, Fingerprint } from 'lucide-react'

interface ImportPickerModalProps {
  show: boolean
  onClose: () => void
  onSelectTxt: () => void
  onSelectJson: () => void
  onSelectAtTxt: () => void
}

export function ImportPickerModal({
  show,
  onClose,
  onSelectTxt,
  onSelectJson,
  onSelectAtTxt,
}: ImportPickerModalProps) {
  const { t } = useTranslation()

  return (
    <Modal
      show={show}
      title={t('accounts.importTitle')}
      contentClassName="sm:max-w-[640px]"
      onClose={onClose}
    >
      <div className="grid grid-cols-3 gap-3">
        <button
          className="flex items-center gap-3 rounded-xl border border-border px-4 py-3 text-left hover:bg-muted/50 transition-colors"
          onClick={() => {
            onClose()
            onSelectTxt()
          }}
        >
          <FileText className="size-5 shrink-0 text-muted-foreground" />
          <div>
            <div className="text-sm font-medium">{t('accounts.importTxt')}</div>
            <div className="text-[11px] text-muted-foreground">{t('accounts.importTxtDesc')}</div>
          </div>
        </button>
        <button
          className="flex items-center gap-3 rounded-xl border border-border px-4 py-3 text-left hover:bg-muted/50 transition-colors"
          onClick={() => {
            onClose()
            onSelectJson()
          }}
        >
          <FileJson className="size-5 shrink-0 text-muted-foreground" />
          <div>
            <div className="text-sm font-medium">{t('accounts.importJson')}</div>
            <div className="text-[11px] text-muted-foreground">{t('accounts.importJsonDesc')}</div>
          </div>
        </button>
        <button
          className="flex items-center gap-3 rounded-xl border border-border px-4 py-3 text-left hover:bg-muted/50 transition-colors"
          onClick={() => {
            onClose()
            onSelectAtTxt()
          }}
        >
          <Fingerprint className="size-5 shrink-0 text-muted-foreground" />
          <div>
            <div className="text-sm font-medium">{t('accounts.importAtTxt')}</div>
            <div className="text-[11px] text-muted-foreground">{t('accounts.importAtTxtDesc')}</div>
          </div>
        </button>
      </div>
    </Modal>
  )
}

interface ExportPickerModalProps {
  show: boolean
  onClose: () => void
  selectedCount: number
  onExport: (format: 'json' | 'txt', scope: 'healthy' | 'selected') => void
}

export function ExportPickerModal({
  show,
  onClose,
  selectedCount,
  onExport,
}: ExportPickerModalProps) {
  const { t } = useTranslation()

  return (
    <Modal
      show={show}
      title={t('accounts.exportTitle')}
      contentClassName="sm:max-w-[580px]"
      onClose={onClose}
    >
      <div className="grid grid-cols-2 gap-3">
        <button
          className="flex items-center gap-3 rounded-xl border border-border px-4 py-3.5 text-left hover:bg-muted/50 transition-colors"
          onClick={() => onExport('json', 'healthy')}
        >
          <FileJson className="size-5 shrink-0 text-muted-foreground" />
          <div className="min-w-0">
            <div className="text-sm font-medium whitespace-nowrap">{t('accounts.exportHealthyJson')}</div>
            <div className="text-[11px] text-muted-foreground">{t('accounts.exportHealthyJsonDesc')}</div>
          </div>
        </button>
        <button
          className="flex items-center gap-3 rounded-xl border border-border px-4 py-3.5 text-left hover:bg-muted/50 transition-colors"
          onClick={() => onExport('txt', 'healthy')}
        >
          <FileText className="size-5 shrink-0 text-muted-foreground" />
          <div className="min-w-0">
            <div className="text-sm font-medium whitespace-nowrap">{t('accounts.exportHealthyTxt')}</div>
            <div className="text-[11px] text-muted-foreground">{t('accounts.exportHealthyTxtDesc')}</div>
          </div>
        </button>
        <button
          className="flex items-center gap-3 rounded-xl border border-border px-4 py-3.5 text-left hover:bg-muted/50 transition-colors disabled:opacity-40 disabled:pointer-events-none"
          disabled={selectedCount === 0}
          onClick={() => onExport('json', 'selected')}
        >
          <FileJson className="size-5 shrink-0 text-muted-foreground" />
          <div className="min-w-0">
            <div className="text-sm font-medium whitespace-nowrap">{t('accounts.exportSelectedJson')}</div>
            <div className="text-[11px] text-muted-foreground">{t('accounts.exportSelectedJsonDesc')}</div>
          </div>
        </button>
        <button
          className="flex items-center gap-3 rounded-xl border border-border px-4 py-3.5 text-left hover:bg-muted/50 transition-colors disabled:opacity-40 disabled:pointer-events-none"
          disabled={selectedCount === 0}
          onClick={() => onExport('txt', 'selected')}
        >
          <FileText className="size-5 shrink-0 text-muted-foreground" />
          <div className="min-w-0">
            <div className="text-sm font-medium whitespace-nowrap">{t('accounts.exportSelectedTxt')}</div>
            <div className="text-[11px] text-muted-foreground">{t('accounts.exportSelectedTxtDesc')}</div>
          </div>
        </button>
      </div>
    </Modal>
  )
}

interface ImportProgressModalProps {
  show: boolean
  done: boolean
  current: number
  total: number
  success: number
  duplicate: number
  failed: number
  onClose: () => void
}

export function ImportProgressModal({
  show,
  done,
  current,
  total,
  success,
  duplicate,
  failed,
  onClose,
}: ImportProgressModalProps) {
  const { t } = useTranslation()

  return (
    <Modal
      show={show}
      title={done ? t('accounts.importDone') : t('accounts.importingProgress')}
      contentClassName="sm:max-w-[420px]"
      onClose={onClose}
    >
      <div className="space-y-4">
        <div className="w-full h-3 bg-muted rounded-full overflow-hidden">
          <div
            className="h-full bg-primary rounded-full transition-all duration-300 ease-out"
            style={{ width: total > 0 ? `${Math.round((current / total) * 100)}%` : '0%' }}
          />
        </div>
        <div className="text-center text-sm text-muted-foreground">
          {total > 0
            ? `${current} / ${total}  (${Math.round((current / total) * 100)}%)`
            : t('accounts.importPreparing')}
        </div>
        <div className="grid grid-cols-3 gap-3 text-center">
          <div className="rounded-xl bg-emerald-500/10 px-3 py-2">
            <div className="text-lg font-bold text-emerald-600">{success}</div>
            <div className="text-[11px] text-muted-foreground">{t('accounts.importSuccess')}</div>
          </div>
          <div className="rounded-xl bg-amber-500/10 px-3 py-2">
            <div className="text-lg font-bold text-amber-600">{duplicate}</div>
            <div className="text-[11px] text-muted-foreground">{t('accounts.importDuplicate')}</div>
          </div>
          <div className="rounded-xl bg-red-500/10 px-3 py-2">
            <div className="text-lg font-bold text-red-600">{failed}</div>
            <div className="text-[11px] text-muted-foreground">{t('accounts.importFailedCount')}</div>
          </div>
        </div>
        {done && (
          <p className="text-xs text-center text-muted-foreground">{t('accounts.importDoneHint')}</p>
        )}
      </div>
    </Modal>
  )
}
