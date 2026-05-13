import type { ChangeEvent } from 'react'
import { useTranslation } from 'react-i18next'
import Modal from '../../../components/Modal'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'

interface MigrateModalProps {
  show: boolean
  onClose: () => void
  url: string
  setUrl: (url: string) => void
  adminKey: string
  setAdminKey: (key: string) => void
  migrating: boolean
  onConfirm: () => void
}

export function MigrateModal({
  show,
  onClose,
  url,
  setUrl,
  adminKey,
  setAdminKey,
  migrating,
  onConfirm,
}: MigrateModalProps) {
  const { t } = useTranslation()

  return (
    <Modal
      show={show}
      title={t('accounts.migrateTitle')}
      contentClassName="sm:max-w-[520px]"
      onClose={onClose}
    >
      <div className="space-y-4">
        <div className="rounded-xl border border-border bg-muted/30 px-4 py-3 text-sm text-muted-foreground">
          <p>{t('accounts.migrateDesc')}</p>
        </div>
        <div>
          <label className="block mb-2 text-sm font-semibold text-muted-foreground">{t('accounts.migrateUrlLabel')}</label>
          <Input
            placeholder={t('accounts.migrateUrlPlaceholder')}
            value={url}
            onChange={(e: ChangeEvent<HTMLInputElement>) => setUrl(e.target.value)}
          />
        </div>
        <div>
          <label className="block mb-2 text-sm font-semibold text-muted-foreground">{t('accounts.migrateKeyLabel')}</label>
          <Input
            type="password"
            placeholder={t('accounts.migrateKeyPlaceholder')}
            value={adminKey}
            onChange={(e: ChangeEvent<HTMLInputElement>) => setAdminKey(e.target.value)}
          />
        </div>
        <div className="flex justify-end gap-2 pt-2">
          <Button variant="outline" onClick={onClose}>
            {t('common.cancel')}
          </Button>
          <Button
            onClick={onConfirm}
            disabled={migrating || !url.trim() || !adminKey.trim()}
          >
            {migrating ? t('accounts.migrating') : t('accounts.migrateConfirm')}
          </Button>
        </div>
      </div>
    </Modal>
  )
}
