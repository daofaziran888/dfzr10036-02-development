import { request } from './client'
import { moduleMeta } from '@/data/modules'
import type { ActionResult, EntryRow, OverviewResult, PageResult } from '@/data/types'

export { moduleMeta }

export async function listEntries(
  key: string,
  filters: Record<string, string> = {},
): Promise<PageResult> {
  const query = new URLSearchParams(filters).toString()
  const response = await request(`/api/${key}?${query}`)
  if (!response.ok) throw new Error('业务数据读取失败')
  return await response.json()
}

export async function createEntry(
  key: string,
  values: Record<string, string>,
): Promise<ActionResult> {
  const response = await request(`/api/${key}`, {
    method: 'POST',
    body: JSON.stringify({ values }),
  })
  return await response.json()
}

export async function runAction(
  key: string,
  id: number,
  action: string,
): Promise<ActionResult> {
  const response = await request(`/api/${key}/${id}/actions`, {
    method: 'POST',
    body: JSON.stringify({ action }),
  })
  return await response.json()
}

export async function downloadEntries(
  key: string,
  filters: Record<string, string> = {},
): Promise<void> {
  const query = new URLSearchParams(filters).toString()
  const response = await request(`/api/${key}/export?${query}`)
  if (!response.ok) throw new Error('业务数据导出失败')
  const url = URL.createObjectURL(await response.blob())
  const anchor = document.createElement('a')
  anchor.href = url
  anchor.download = `${key}.csv`
  document.body.appendChild(anchor)
  anchor.click()
  document.body.removeChild(anchor)
  URL.revokeObjectURL(url)
}

export async function loadOverview(): Promise<OverviewResult> {
  const response = await request('/api/overview')
  if (!response.ok) throw new Error('运营概览读取失败')
  return await response.json()
}

// ---- 同键事件校验清单 ----

export async function listCheckWindows(): Promise<{ items: Record<string, unknown>[]; total: number }> {
  const response = await request('/api/key_check/windows')
  if (!response.ok) throw new Error('校验窗口读取失败')
  return await response.json()
}

export async function loadCheckWindow(windowId: string): Promise<{
  window: Record<string, unknown>
  events: Record<string, unknown>[]
}> {
  const response = await request(`/api/key_check/windows/${windowId}`)
  if (!response.ok) throw new Error('校验窗口明细读取失败')
  return await response.json()
}

export async function runCheckWindow(
  windowId: string,
  threshold: Record<string, unknown> = {},
  thresholdVersion = '',
): Promise<{ ok: boolean; message: string }> {
  const response = await request(`/api/key_check/windows/${windowId}/run`, {
    method: 'POST',
    body: JSON.stringify({ threshold, threshold_version: thresholdVersion || null }),
  })
  return await response.json()
}

export async function listCheckAudits(windowId: string): Promise<{ items: Record<string, unknown>[] }> {
  const query = new URLSearchParams({ window_id: windowId }).toString()
  const response = await request(`/api/key_check/audits?${query}`)
  if (!response.ok) throw new Error('校验审计读取失败')
  return await response.json()
}

// ---- 维表关联消费校验清单 ----

export async function loadDimKeyCheck(entryId: number): Promise<Record<string, unknown>> {
  const response = await request(`/api/dim_join/${entryId}/key-check`)
  if (!response.ok) throw new Error('维表关联校验清单读取失败')
  return await response.json()
}

export async function buildDimJoinRows(entryId: number): Promise<{
  ok: boolean
  message: string
  result: Record<string, unknown>
}> {
  const response = await request(`/api/dim_join/${entryId}/build-rows`, { method: 'POST' })
  return await response.json()
}

export async function loadDimJoinRows(entryId: number): Promise<{
  rows: Record<string, unknown>[]
  recompute_tasks: Record<string, unknown>[]
}> {
  const response = await request(`/api/dim_join/${entryId}/join-rows`)
  if (!response.ok) throw new Error('维表关联数据行读取失败')
  return await response.json()
}

export async function completeRecompute(taskId: number): Promise<{ ok: boolean; message: string }> {
  const response = await request(`/api/dim_join/recompute/${taskId}/complete`, { method: 'POST' })
  return await response.json()
}
