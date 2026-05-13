import { useState } from 'react'

export function useModalState() {
  const [showAdd, setShowAdd] = useState(false)
  const [showImportPicker, setShowImportPicker] = useState(false)
  const [showExportPicker, setShowExportPicker] = useState(false)
  const [showMigrate, setShowMigrate] = useState(false)

  return {
    showAdd,
    setShowAdd,
    showImportPicker,
    setShowImportPicker,
    showExportPicker,
    setShowExportPicker,
    showMigrate,
    setShowMigrate,
  }
}
