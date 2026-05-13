import type { AccountRow } from '../../types'

export interface AccountsState {
  showAdd: boolean
  page: number
  statusFilter: 'all' | 'normal' | 'rate_limited' | 'banned'
  searchQuery: string
  planFilter: 'all' | 'pro' | 'team' | 'free'
  sortKey: 'requests' | 'usage' | 'importTime' | null
  sortDir: 'asc' | 'desc'
  selected: Set<number>
  refreshingIds: Set<number>
  testingAccount: AccountRow | null
  usageAccount: AccountRow | null
}

export interface LoadingState {
  submitting: boolean
  batchLoading: boolean
  batchTesting: boolean
  batchRefreshing: boolean
  cleaningBanned: boolean
  cleaningRateLimited: boolean
  cleaningError: boolean
  importing: boolean
  exporting: boolean
  migrating: boolean
}

export interface ModalState {
  showAdd: boolean
  showImportPicker: boolean
  showExportPicker: boolean
  showMigrate: boolean
}

export interface ImportProgress {
  show: boolean
  current: number
  total: number
  success: number
  duplicate: number
  failed: number
  done: boolean
}

export interface AddFormState {
  addMethod: 'rt' | 'at' | 'oauth'
  rtForm: { refresh_token: string; proxy_url: string }
  atForm: { access_token: string; proxy_url: string }
  oauthStep: 'generate' | 'exchange'
  oauthSession: { session_id: string; auth_url: string } | null
  oauthProxyUrl: string
  oauthCallbackUrl: string
  oauthName: string
  oauthGenerating: boolean
  oauthCompleting: boolean
}

export interface MigrateState {
  url: string
  key: string
}
