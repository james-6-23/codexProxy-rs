/// <reference types="vite/client" />

declare const __APP_VERSION__: string

interface Document {
  startViewTransition?: (callback: () => void) => {
    ready: Promise<void>
    finished: Promise<void>
    updateCallbackDone: Promise<void>
  }
}
