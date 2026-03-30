import { useCallback, useEffect, useState } from 'react'

type Theme = 'light' | 'dark'

const STORAGE_KEY = 'theme'

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

  const toggle = useCallback((e?: React.MouseEvent) => {
    const root = document.documentElement
    const newTheme: Theme = root.classList.contains('dark') ? 'light' : 'dark'

    // 获取点击坐标（默认左下角）
    const x = e?.clientX ?? 40
    const y = e?.clientY ?? window.innerHeight - 40

    // 计算扩散半径（到最远角的距离）
    const endRadius = Math.hypot(
      Math.max(x, window.innerWidth - x),
      Math.max(y, window.innerHeight - y),
    )

    // 优先使用 View Transition API（Chrome 111+, Safari 18+）
    if (document.startViewTransition) {
      const transition = document.startViewTransition(() => {
        setThemeState(newTheme)
      })

      transition.ready.then(() => {
        root.animate(
          {
            clipPath: [
              `circle(0px at ${x}px ${y}px)`,
              `circle(${endRadius}px at ${x}px ${y}px)`,
            ],
          },
          {
            duration: 500,
            easing: 'ease-out',
            pseudoElement: '::view-transition-new(root)',
          },
        )
      })
    } else {
      // 降级：直接切换，无动画
      setThemeState(newTheme)
    }
  }, [])

  return { theme, toggle }
}
