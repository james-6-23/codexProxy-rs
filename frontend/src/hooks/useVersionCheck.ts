import { useEffect, useState } from 'react'

const GITHUB_API = 'https://api.github.com/repos/james-6-23/codex2api/releases/latest'
const CACHE_KEY = 'codex2api_latest_version'
const CACHE_TTL = 10 * 60 * 1000 // 10 分钟缓存
const POLL_INTERVAL = 30 * 60 * 1000 // 30 分钟轮询

interface CachedVersion {
  version: string
  checkedAt: number
}

/** 解析版本号字符串为数字数组，如 "v1.0.5" → [1, 0, 5] */
function parseVersion(tag: string): number[] | null {
  const m = tag.replace(/^v/i, '').match(/^(\d+)\.(\d+)\.(\d+)/)
  if (!m) return null
  return [Number(m[1]), Number(m[2]), Number(m[3])]
}

/** 判断 remote 是否比 local 更新 */
function isNewer(remote: number[], local: number[]): boolean {
  for (let i = 0; i < 3; i++) {
    if (remote[i] > local[i]) return true
    if (remote[i] < local[i]) return false
  }
  return false
}

async function fetchLatestVersion(): Promise<string | null> {
  // 优先读取未过期的缓存
  try {
    const raw = localStorage.getItem(CACHE_KEY)
    if (raw) {
      const cached: CachedVersion = JSON.parse(raw)
      if (Date.now() - cached.checkedAt < CACHE_TTL) {
        return cached.version
      }
    }
  } catch { /* 缓存损坏忽略 */ }

  try {
    const res = await fetch(GITHUB_API, {
      headers: { Accept: 'application/vnd.github.v3+json' },
      signal: AbortSignal.timeout(10000),
    })
    if (!res.ok) return null
    const data = await res.json()
    const version = data.tag_name as string
    if (version) {
      localStorage.setItem(CACHE_KEY, JSON.stringify({ version, checkedAt: Date.now() }))
    }
    return version || null
  } catch {
    return null
  }
}

export function useVersionCheck() {
  const [latestVersion, setLatestVersion] = useState<string | null>(null)
  const [hasUpdate, setHasUpdate] = useState(false)

  useEffect(() => {
    const currentVersion = __APP_VERSION__
    // 开发模式不检查
    if (currentVersion === 'dev') return

    const localParsed = parseVersion(currentVersion)
    if (!localParsed) return

    const check = async () => {
      const remote = await fetchLatestVersion()
      if (!remote) return
      const remoteParsed = parseVersion(remote)
      if (!remoteParsed) return

      setLatestVersion(remote)
      setHasUpdate(isNewer(remoteParsed, localParsed))
    }

    void check()
    const timer = setInterval(() => void check(), POLL_INTERVAL)
    return () => clearInterval(timer)
  }, [])

  return { hasUpdate, latestVersion }
}
