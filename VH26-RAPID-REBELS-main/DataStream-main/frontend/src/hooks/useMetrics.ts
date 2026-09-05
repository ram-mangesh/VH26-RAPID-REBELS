import { useCallback, useEffect, useRef, useState } from 'react'
import {
  fetchErrorRate,
  fetchGeneratorRate,
  fetchOrdersPerMinute,
  fetchRevenueByRegion,
  fetchTopProducts,
  fetchRealtimeRate,
} from '../api/client'
import type { MetricsState } from '../types'

const POLL_INTERVAL_MS = 3_000
const REALTIME_POLL_MS = 1_000

export function useMetrics(): MetricsState {
  const [state, setState] = useState<MetricsState>({
    ordersPerMinute: [],
    revenueByRegion: [],
    topProducts: [],
    errorRate: null,
    generatorRate: null,
    realtimeRate: null,
    loading: true,
    lastUpdated: null,
  })

  const intervalRef = useRef<ReturnType<typeof setInterval> | null>(null)
  const realtimeIntervalRef = useRef<ReturnType<typeof setInterval> | null>(null)

  const refresh = useCallback(async () => {
    try {
      const [ordersPerMinute, revenueByRegion, topProducts, errorRate, generatorRate] = await Promise.all([
        fetchOrdersPerMinute(),
        fetchRevenueByRegion(),
        fetchTopProducts(),
        fetchErrorRate(),
        fetchGeneratorRate(),
      ])
      setState({
        ordersPerMinute,
        revenueByRegion,
        topProducts,
        errorRate,
        generatorRate,
        realtimeRate: null,
        loading: false,
        lastUpdated: new Date(),
      })
    } catch (err) {
      console.error('Failed to fetch metrics:', err)
      setState(prev => ({ ...prev, loading: false }))
    }
  }, [])

  const refreshRealtime = useCallback(async () => {
    try {
      const realtimeRate = await fetchRealtimeRate()
      setState(prev => ({ ...prev, realtimeRate }))
    } catch {
      // ignore realtime errors
    }
  }, [])

  useEffect(() => {
    refresh()
    intervalRef.current = setInterval(refresh, POLL_INTERVAL_MS)
    realtimeIntervalRef.current = setInterval(refreshRealtime, REALTIME_POLL_MS)
    return () => {
      if (intervalRef.current) clearInterval(intervalRef.current)
      if (realtimeIntervalRef.current) clearInterval(realtimeIntervalRef.current)
    }
  }, [refresh, refreshRealtime])

  return state
}
