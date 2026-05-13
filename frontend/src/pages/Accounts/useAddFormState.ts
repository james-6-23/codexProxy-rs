import { useState } from 'react'
import type { AddAccountRequest, AddATAccountRequest } from '../../types'

export function useAddFormState() {
  const [addMethod, setAddMethod] = useState<'rt' | 'at' | 'oauth'>('rt')
  const [addForm, setAddForm] = useState<AddAccountRequest>({
    refresh_token: '',
    proxy_url: '',
  })
  const [atForm, setAtForm] = useState<AddATAccountRequest>({
    access_token: '',
    proxy_url: '',
  })
  const [oauthStep, setOauthStep] = useState<'generate' | 'exchange'>('generate')
  const [oauthSession, setOauthSession] = useState<{ session_id: string; auth_url: string } | null>(null)
  const [oauthProxyUrl, setOauthProxyUrl] = useState('')
  const [oauthCallbackUrl, setOauthCallbackUrl] = useState('')
  const [oauthName, setOauthName] = useState('')
  const [oauthGenerating, setOauthGenerating] = useState(false)
  const [oauthCompleting, setOauthCompleting] = useState(false)

  const resetAddForm = () => {
    setAddMethod('rt')
    setAddForm({ refresh_token: '', proxy_url: '' })
    setAtForm({ access_token: '', proxy_url: '' })
    setOauthStep('generate')
    setOauthSession(null)
    setOauthProxyUrl('')
    setOauthCallbackUrl('')
    setOauthName('')
  }

  return {
    addMethod,
    setAddMethod,
    addForm,
    setAddForm,
    atForm,
    setAtForm,
    oauthStep,
    setOauthStep,
    oauthSession,
    setOauthSession,
    oauthProxyUrl,
    setOauthProxyUrl,
    oauthCallbackUrl,
    setOauthCallbackUrl,
    oauthName,
    setOauthName,
    oauthGenerating,
    setOauthGenerating,
    oauthCompleting,
    setOauthCompleting,
    resetAddForm,
  }
}
