import { useState, useEffect, useRef, useCallback } from 'react'
import { useTranslation } from 'react-i18next'
import type { AccountRow } from '../../../types'
import { getAdminKey } from '../../../api'
import Modal from '../../../components/Modal'
import { Button } from '@/components/ui/button'
import { formatTestErrorMessage } from '../formatters'

interface TestEvent {
  type: 'test_start' | 'content' | 'test_complete' | 'error'
  text?: string
  model?: string
  success?: boolean
  error?: string
}

interface TestConnectionModalProps {
  account: AccountRow
  onClose: () => void
  onSettled: () => void
}

export function TestConnectionModal({ account, onClose, onSettled }: TestConnectionModalProps) {
  const { t } = useTranslation()
  const [output, setOutput] = useState<string[]>([])
  const [status, setStatus] = useState<'connecting' | 'streaming' | 'success' | 'error'>('connecting')
  const [errorMsg, setErrorMsg] = useState('')
  const [model, setModel] = useState('')
  const abortRef = useRef<AbortController | null>(null)
  const outputEndRef = useRef<HTMLDivElement>(null)
  const settledRef = useRef(false)
  const onSettledRef = useRef(onSettled)
  onSettledRef.current = onSettled

  const markSettled = useCallback(() => {
    if (settledRef.current) return
    settledRef.current = true
    onSettledRef.current()
  }, [])

  useEffect(() => {
    setOutput([])
    setStatus('connecting')
    setErrorMsg('')
    settledRef.current = false

    const controller = new AbortController()
    abortRef.current = controller

    const run = async () => {
      if (controller.signal.aborted) return

      try {
        const res = await fetch(`/api/admin/accounts/${account.id}/test`, {
          signal: controller.signal,
          headers: getAdminKey() ? { 'X-Admin-Key': getAdminKey() } : {},
        })

        if (!res.ok) {
          const body = await res.text()
          let msg = `HTTP ${res.status}`
          try {
            const parsed = JSON.parse(body)
            if (parsed.error) msg = parsed.error
          } catch { /* ignore */ }
          setStatus('error')
          setErrorMsg(msg)
          markSettled()
          return
        }

        const reader = res.body?.getReader()
        if (!reader) {
          setStatus('error')
          setErrorMsg(t('accounts.browserStreamingUnsupported'))
          markSettled()
          return
        }

        const decoder = new TextDecoder()
        let buffer = ''
        let receivedTerminalEvent = false

        const processEventLines = (lines: string[]) => {
          for (const line of lines) {
            const trimmed = line.trim()
            if (!trimmed.startsWith('data: ')) continue

            try {
              const event: TestEvent = JSON.parse(trimmed.slice(6))

              switch (event.type) {
                case 'test_start':
                  setModel(event.model || '')
                  setStatus('streaming')
                  break
                case 'content':
                  if (event.text) {
                    setOutput((prev) => [...prev, event.text!])
                  }
                  break
                case 'test_complete':
                  receivedTerminalEvent = true
                  setStatus(event.success ? 'success' : 'error')
                  markSettled()
                  break
                case 'error':
                  receivedTerminalEvent = true
                  setStatus('error')
                  setErrorMsg(event.error || t('accounts.unknownError'))
                  markSettled()
                  break
              }
            } catch { /* ignore non-JSON lines */ }
          }
        }

        while (true) {
          const { done, value } = await reader.read()
          if (done) {
            buffer += decoder.decode()
            break
          }

          buffer += decoder.decode(value, { stream: true })
          const lines = buffer.split('\n')
          buffer = lines.pop() || ''
          processEventLines(lines)
        }

        if (buffer.trim()) {
          processEventLines([buffer])
        }

        if (!receivedTerminalEvent) {
          setStatus('error')
          setErrorMsg(t('accounts.connectionEndedUnexpectedly'))
          markSettled()
        }
      } catch (err: unknown) {
        if (err instanceof DOMException && err.name === 'AbortError') return
        setStatus('error')
        setErrorMsg(err instanceof Error ? err.message : t('accounts.connectionFailed'))
        markSettled()
      }
    }

    const timer = window.setTimeout(() => {
      void run()
    }, 50)

    return () => {
      window.clearTimeout(timer)
      controller.abort()
    }
  }, [account.id, markSettled, t])

  useEffect(() => {
    outputEndRef.current?.scrollIntoView({ behavior: 'smooth' })
  }, [output])

  const statusLabel = {
    connecting: `⏳ ${t('accounts.connecting')}`,
    streaming: `🔄 ${t('accounts.receivingResponse')}`,
    success: `✅ ${t('accounts.testSuccess')}`,
    error: `❌ ${t('accounts.testFailed')}`,
  }[status]

  const statusColor = {
    connecting: 'text-muted-foreground',
    streaming: 'text-blue-500',
    success: 'text-emerald-500',
    error: 'text-red-500',
  }[status]

  const formattedErrorMsg = errorMsg ? formatTestErrorMessage(errorMsg) : ''

  return (
    <Modal
      show={true}
      title={t('accounts.testConnectionTitle', { account: account.email || `ID ${account.id}` })}
      onClose={() => {
        abortRef.current?.abort()
        onClose()
      }}
      footer={
        <Button
          variant="outline"
          onClick={() => {
            abortRef.current?.abort()
            onClose()
          }}
        >
          {t('common.close')}
        </Button>
      }
      contentClassName="sm:max-w-[680px]"
    >
      <div className="space-y-4">
        <div className="flex flex-wrap items-start justify-between gap-2">
          <span className={`flex items-center gap-1.5 text-sm font-semibold ${statusColor}`}>
            {statusLabel}
          </span>
          {model && (
            <span className="max-w-full rounded-md bg-muted px-2 py-0.5 font-mono text-xs break-all text-muted-foreground">
              {model}
            </span>
          )}
        </div>

        {(output.length > 0 || status === 'connecting' || status === 'streaming') && (
          <div
            className="min-h-[80px] max-h-[240px] overflow-auto rounded-xl border border-border bg-muted/30 p-3 text-[20px] leading-[1.8] whitespace-pre-wrap break-all"
            style={{ fontFamily: 'var(--font-geist-mono)' }}
          >
            {output.length === 0 && status === 'connecting' && (
              <span className="text-muted-foreground animate-pulse">{t('accounts.sendingTestRequest')}</span>
            )}
            {output.join('')}
            <div ref={outputEndRef} />
          </div>
        )}

        {errorMsg && (
          <div className="max-h-[40vh] overflow-auto rounded-xl border border-red-200 bg-red-50 p-3.5 text-red-600 dark:border-red-900/50 dark:bg-red-950/30 dark:text-red-400">
            <div className="mb-2 text-sm font-semibold">{t('accounts.failureDetails')}</div>
            <pre
              className="text-[20px] leading-[1.8] whitespace-pre-wrap break-all"
              style={{ fontFamily: 'var(--font-geist-mono)' }}
            >
              {formattedErrorMsg}
            </pre>
          </div>
        )}
      </div>
    </Modal>
  )
}
