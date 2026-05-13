import { lazy, Suspense } from 'react'
import { Route, Routes } from 'react-router-dom'
import AuthGate from './components/AuthGate'
import Layout from './components/Layout'

const Dashboard = lazy(() => import('./pages/Dashboard'))
const Accounts = lazy(() => import('./pages/Accounts'))
const Proxies = lazy(() => import('./pages/Proxies'))
const Operations = lazy(() => import('./pages/Operations'))
const SchedulerBoard = lazy(() => import('./pages/SchedulerBoard'))
const Usage = lazy(() => import('./pages/Usage'))
const Settings = lazy(() => import('./pages/Settings'))

function RouteFallback() {
  return (
    <div className="flex items-center justify-center p-12 text-muted-foreground">
      <div className="size-6 rounded-full border-2 border-muted-foreground/30 border-t-muted-foreground animate-spin" />
    </div>
  )
}

export default function App() {
  return (
    <AuthGate>
      <Layout>
        <Suspense fallback={<RouteFallback />}>
          <Routes>
            <Route path="/" element={<Dashboard />} />
            <Route path="/accounts" element={<Accounts />} />
            <Route path="/proxies" element={<Proxies />} />
            <Route path="/ops" element={<Operations />} />
            <Route path="/ops/scheduler" element={<SchedulerBoard />} />
            <Route path="/usage" element={<Usage />} />
            <Route path="/settings" element={<Settings />} />
          </Routes>
        </Suspense>
      </Layout>
    </AuthGate>
  )
}
