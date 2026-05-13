import { useState } from 'react'

export function useLoadingState() {
  const [submitting, setSubmitting] = useState(false)
  const [batchLoading, setBatchLoading] = useState(false)
  const [batchTesting, setBatchTesting] = useState(false)
  const [batchRefreshing, setBatchRefreshing] = useState(false)
  const [cleaningBanned, setCleaningBanned] = useState(false)
  const [cleaningRateLimited, setCleaningRateLimited] = useState(false)
  const [cleaningError, setCleaningError] = useState(false)
  const [importing, setImporting] = useState(false)
  const [exporting, setExporting] = useState(false)
  const [migrating, setMigrating] = useState(false)

  return {
    submitting,
    setSubmitting,
    batchLoading,
    setBatchLoading,
    batchTesting,
    setBatchTesting,
    batchRefreshing,
    setBatchRefreshing,
    cleaningBanned,
    setCleaningBanned,
    cleaningRateLimited,
    setCleaningRateLimited,
    cleaningError,
    setCleaningError,
    importing,
    setImporting,
    exporting,
    setExporting,
    migrating,
    setMigrating,
  }
}
