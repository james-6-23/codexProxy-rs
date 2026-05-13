import type { ChangeEvent } from 'react'
import { useTranslation } from 'react-i18next'
import Modal from '../../../components/Modal'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { RefreshCw, Fingerprint, KeyRound, ExternalLink } from 'lucide-react'

interface AddAccountModalProps {
  show: boolean
  onClose: () => void
  addMethod: 'rt' | 'at' | 'oauth'
  setAddMethod: (method: 'rt' | 'at' | 'oauth') => void
  rtForm: { refresh_token: string; proxy_url: string }
  setRtForm: (form: { refresh_token: string; proxy_url: string }) => void
  atForm: { access_token: string; proxy_url: string }
  setAtForm: (form: { access_token: string; proxy_url: string }) => void
  oauthStep: 'generate' | 'exchange'
  setOauthStep: (step: 'generate' | 'exchange') => void
  oauthSession: { session_id: string; auth_url: string } | null
  oauthProxyUrl: string
  setOauthProxyUrl: (url: string) => void
  oauthCallbackUrl: string
  setOauthCallbackUrl: (url: string) => void
  oauthName: string
  setOauthName: (name: string) => void
  oauthGenerating: boolean
  oauthCompleting: boolean
  submitting: boolean
  onSubmitRT: () => void
  onSubmitAT: () => void
  onOAuthGenerate: () => void
  onOAuthComplete: () => void
}

export function AddAccountModal({
  show,
  onClose,
  addMethod,
  setAddMethod,
  rtForm,
  setRtForm,
  atForm,
  setAtForm,
  oauthStep,
  setOauthStep,
  oauthSession,
  oauthProxyUrl,
  setOauthProxyUrl,
  oauthCallbackUrl,
  setOauthCallbackUrl,
  oauthName,
  setOauthName,
  oauthGenerating,
  oauthCompleting,
  submitting,
  onSubmitRT,
  onSubmitAT,
  onOAuthGenerate,
  onOAuthComplete,
}: AddAccountModalProps) {
  const { t } = useTranslation()

  return (
    <Modal
      show={show}
      title={t('accounts.addTitle')}
      contentClassName="sm:max-w-[640px]"
      onClose={onClose}
      footer={(
        <>
          <Button variant="outline" onClick={onClose}>
            {t('common.cancel')}
          </Button>
          {addMethod === 'rt' ? (
            <Button onClick={onSubmitRT} disabled={submitting || !rtForm.refresh_token.trim()}>
              {submitting ? t('accounts.adding') : t('accounts.submit')}
            </Button>
          ) : addMethod === 'at' ? (
            <Button onClick={onSubmitAT} disabled={submitting || !atForm.access_token.trim()}>
              {submitting ? t('accounts.adding') : t('accounts.submit')}
            </Button>
          ) : oauthStep === 'generate' ? (
            <Button onClick={onOAuthGenerate} disabled={oauthGenerating}>
              {oauthGenerating ? t('accounts.oauthGenerating') : t('accounts.oauthGenerateBtn')}
            </Button>
          ) : (
            <Button onClick={onOAuthComplete} disabled={oauthCompleting || !oauthCallbackUrl.trim()}>
              {oauthCompleting ? t('accounts.oauthCompleting') : t('accounts.oauthCompleteBtn')}
            </Button>
          )}
        </>
      )}
    >
      {/* Tab switcher */}
      <div className="flex gap-1 p-1 mb-5 rounded-xl bg-muted/50 border border-border">
        <button
          onClick={() => setAddMethod('rt')}
          className={`flex-1 flex items-center justify-center gap-1.5 rounded-lg py-2 text-sm font-semibold transition-all ${
            addMethod === 'rt'
              ? 'bg-background shadow-sm text-foreground'
              : 'text-muted-foreground hover:text-foreground'
          }`}
        >
          <RefreshCw className="size-3.5" />
          {t('accounts.addMethodRT')}
        </button>
        <button
          onClick={() => setAddMethod('at')}
          className={`flex-1 flex items-center justify-center gap-1.5 rounded-lg py-2 text-sm font-semibold transition-all ${
            addMethod === 'at'
              ? 'bg-background shadow-sm text-foreground'
              : 'text-muted-foreground hover:text-foreground'
          }`}
        >
          <Fingerprint className="size-3.5" />
          {t('accounts.addMethodAT')}
        </button>
        <button
          onClick={() => { setAddMethod('oauth'); setOauthStep('generate') }}
          className={`flex-1 flex items-center justify-center gap-1.5 rounded-lg py-2 text-sm font-semibold transition-all ${
            addMethod === 'oauth'
              ? 'bg-background shadow-sm text-foreground'
              : 'text-muted-foreground hover:text-foreground'
          }`}
        >
          <KeyRound className="size-3.5" />
          {t('accounts.addMethodOAuth')}
        </button>
      </div>

      {addMethod === 'rt' ? (
        <div className="space-y-4">
          <div>
            <label className="block mb-2 text-sm font-semibold text-muted-foreground">{t('accounts.refreshTokenLabel')} *</label>
            <textarea
              className="w-full min-h-[160px] p-3 border border-input rounded-xl bg-background text-sm resize-y focus:outline-none focus:ring-2 focus:ring-ring"
              placeholder={t('accounts.refreshTokenPlaceholder')}
              value={rtForm.refresh_token}
              onChange={(e: ChangeEvent<HTMLTextAreaElement>) =>
                setRtForm({ ...rtForm, refresh_token: e.target.value })
              }
              rows={6}
            />
          </div>
          <div>
            <label className="block mb-2 text-sm font-semibold text-muted-foreground">{t('accounts.proxyUrl')}</label>
            <Input
              placeholder={t('accounts.proxyUrlPlaceholder')}
              value={rtForm.proxy_url}
              onChange={(e: ChangeEvent<HTMLInputElement>) =>
                setRtForm({ ...rtForm, proxy_url: e.target.value })
              }
            />
          </div>
        </div>
      ) : addMethod === 'at' ? (
        <div className="space-y-4">
          <div className="rounded-xl border border-amber-200 bg-amber-50 px-4 py-3 text-sm text-amber-800 dark:border-amber-800 dark:bg-amber-950/50 dark:text-amber-300">
            {t('accounts.atWarning')}
          </div>
          <div>
            <label className="block mb-2 text-sm font-semibold text-muted-foreground">{t('accounts.accessTokenLabel')} *</label>
            <textarea
              className="w-full min-h-[160px] p-3 border border-input rounded-xl bg-background text-sm resize-y focus:outline-none focus:ring-2 focus:ring-ring"
              placeholder={t('accounts.accessTokenPlaceholder')}
              value={atForm.access_token}
              onChange={(e: ChangeEvent<HTMLTextAreaElement>) =>
                setAtForm({ ...atForm, access_token: e.target.value })
              }
              rows={6}
            />
          </div>
          <div>
            <label className="block mb-2 text-sm font-semibold text-muted-foreground">{t('accounts.proxyUrl')}</label>
            <Input
              placeholder={t('accounts.proxyUrlPlaceholder')}
              value={atForm.proxy_url}
              onChange={(e: ChangeEvent<HTMLInputElement>) =>
                setAtForm({ ...atForm, proxy_url: e.target.value })
              }
            />
          </div>
        </div>
      ) : (
        <div className="space-y-5">
          {oauthStep === 'generate' ? (
            <>
              <div className="rounded-xl border border-border bg-muted/30 px-4 py-3 text-sm text-muted-foreground">
                <p className="font-semibold text-foreground mb-1">{t('accounts.oauthStep1Title')}</p>
                <p>{t('accounts.oauthStep1Desc')}</p>
              </div>
              <div>
                <label className="block mb-2 text-sm font-semibold text-muted-foreground">{t('accounts.oauthNameLabel')}</label>
                <Input
                  placeholder={t('accounts.oauthNamePlaceholder')}
                  value={oauthName}
                  onChange={(e: ChangeEvent<HTMLInputElement>) => setOauthName(e.target.value)}
                />
              </div>
              <div>
                <label className="block mb-2 text-sm font-semibold text-muted-foreground">{t('accounts.oauthProxyUrl')}</label>
                <Input
                  placeholder={t('accounts.oauthProxyUrlPlaceholder')}
                  value={oauthProxyUrl}
                  onChange={(e: ChangeEvent<HTMLInputElement>) => setOauthProxyUrl(e.target.value)}
                />
              </div>
            </>
          ) : (
            <>
              <div className="rounded-xl border border-border bg-muted/30 px-4 py-3 text-sm text-muted-foreground">
                <p className="font-semibold text-foreground mb-1">{t('accounts.oauthStep2Title')}</p>
                <p>{t('accounts.oauthStep2Desc')}</p>
              </div>
              {oauthSession && (
                <div className="rounded-xl border border-primary/30 bg-primary/5 px-4 py-3">
                  <p className="text-xs font-semibold text-muted-foreground mb-2">{t('accounts.oauthOpenLink')}</p>
                  <a
                    href={oauthSession.auth_url}
                    target="_blank"
                    rel="noopener noreferrer"
                    className="inline-flex items-center gap-1.5 text-sm font-semibold text-primary hover:underline break-all"
                  >
                    <ExternalLink className="size-3.5 shrink-0" />
                    {t('accounts.oauthOpenLink')}
                  </a>
                </div>
              )}
              <div>
                <label className="block mb-2 text-sm font-semibold text-muted-foreground">{t('accounts.oauthCallbackUrlLabel')}</label>
                <Input
                  placeholder={t('accounts.oauthCallbackUrlPlaceholder')}
                  value={oauthCallbackUrl}
                  onChange={(e: ChangeEvent<HTMLInputElement>) => setOauthCallbackUrl(e.target.value)}
                />
                <p className="mt-1.5 text-xs text-muted-foreground">{t('accounts.oauthCallbackUrlHint')}</p>
              </div>
              <button
                onClick={() => { setOauthStep('generate') }}
                className="text-xs text-muted-foreground hover:text-foreground underline underline-offset-2"
              >
                {t('accounts.oauthRestart')}
              </button>
            </>
          )}
        </div>
      )}
    </Modal>
  )
}
