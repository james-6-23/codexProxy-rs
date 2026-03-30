import { useCallback, useEffect, useState } from 'react'

type Theme = 'light' | 'dark'

const STORAGE_KEY = 'theme'
const TRANSITION_CLASS = 'theme-transition'
const TRANSITION_DURATION = 450

function getInitialTheme(): Theme {
  const stored = localStorage.getItem(STORAGE_KEY) as Theme | null
  if (stored === 'light' || stored === 'dark') return stored
  return window.matchMedia('(prefers-color-scheme: dark)').matches ? 'dark' : 'light'
}

export function useTheme() {
  const [theme, setThemeState] = useState<Theme>(getInitialTheme)

  useEffect(() => {
    const root = document.documentElement
    if (theme === 'dark') {
      root.classList.add('dark')
    } else {
      root.classList.remove('dark')
    }
    localStorage.setItem(STORAGE_KEY, theme)
  }, [theme])

  const toggle = useCallback(() => {
    const root = document.documentElement
    // 启用过渡动画
    root.classList.add(TRANSITION_CLASS)
    setThemeState((t) => (t === 'dark' ? 'light' : 'dark'))
    // 过渡结束后移除，避免影响正常交互性能
    setTimeout(() => root.classList.remove(TRANSITION_CLASS), TRANSITION_DURATION)
  }, [])

  return { theme, toggle }
}
