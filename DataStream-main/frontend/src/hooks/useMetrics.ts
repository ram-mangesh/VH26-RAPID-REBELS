import { useCallback, useEffect, useRef, useState } from 'react'
import {
  fetchErrorRate,
  fetchGeneratorRate,
  fetchOrdersPerMinute,
  fetchRevenueByRegion,
  fetchTopProducts,
} from '../api/client'
import type { MetricsState } from '../types'

const POLL_INTERVAL_MS = 5_000

export function useMetrics(): MetricsState {
  const [state, setState] = useState<MetricsState>({
    ordersPerMinute: [],
    revenueByRegion: [],
    topProducts: [],
    errorRate: null,
    generatorRate: null,
    loading: true,
    lastUpdated: null,
  })

  const intervalRef = useRef<ReturnType<typeof setInterval> | null>(null)

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
        loading: false,
        lastUpdated: new Date(),
      })
    } catch (err) {
      console.error('Failed to fetch metrics:', err)
      setState(prev => ({ ...prev, loading: false }))
    }
  }, [])

  useEffect(() => {
    refresh()
    intervalRef.current = setInterval(refresh, POLL_INTERVAL_MS)
    return () => {
      if (intervalRef.current) clearInterval(intervalRef.current)
    }
  }, [refresh])

  return state
}
