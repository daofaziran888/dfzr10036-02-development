<template>
  <section class="page" :data-module="meta.key">
    <header class="page-head">
      <div>
        <h2>{{ meta.name }}</h2>
        <p class="page-desc">{{ meta.desc }}</p>
      </div>
      <div class="page-actions">
        <button class="btn primary" data-testid="open-create" type="button" @click="openCreate">
          登记{{ meta.entity }}
        </button>
        <button class="btn" data-testid="export" type="button" @click="exportRows">
          导出{{ meta.name }}清单
        </button>
      </div>
    </header>

    <div class="stat-row">
      <article v-for="item in stats" :key="item.label" class="stat-card">
        <span class="stat-label">{{ item.label }}</span>
        <strong class="stat-value">{{ item.value }}</strong>
      </article>
    </div>

    <p class="status-legend">
      <span v-for="item in statusSummary" :key="item.status" class="legend-item">
        {{ item.status }}：{{ item.count }}
      </span>
    </p>

    <form class="filter-bar" @submit.prevent="reload">
      <label v-for="field in filterFields" :key="field" class="filter-item">
        <span>{{ field }}</span>
        <input v-model="filters[field]" :data-testid="`filter-${field}`" :placeholder="`按${field}检索`" />
      </label>
      <button class="btn" data-testid="query" type="submit">查询</button>
      <button class="btn ghost" data-testid="reset" type="button" @click="resetFilters">重置条件</button>
      <button
        v-if="meta.key === 'key_check'"
        class="btn"
        data-testid="run-window"
        type="button"
        :disabled="runningWindow"
        @click="runWindow"
      >
        {{ runningWindow ? '校验中…' : '校验选中窗口' }}
      </button>
    </form>

    <section v-if="meta.key === 'key_check'" class="check-panel" data-testid="check-panel">
      <h3>窗口与审计</h3>
      <table class="data-table">
        <thead>
          <tr>
            <th>窗口编号</th><th>数据流</th><th>窗口区间</th><th>阈值版本</th>
            <th>解析规则版本</th><th>状态</th><th>最近批次</th><th>最近校验</th>
          </tr>
        </thead>
        <tbody>
          <tr
            v-for="window in windows"
            :key="String(window.窗口编号)"
            :class="{ selected: String(window.窗口编号) === selectedWindow }"
            @click="selectWindow(String(window.窗口编号))"
          >
            <td>{{ window.窗口编号 }}</td>
            <td>{{ window.数据流 }}</td>
            <td>{{ window.窗口起点 }} ~ {{ window.窗口终点 }}</td>
            <td>{{ window.阈值版本 }}</td>
            <td>{{ window.解析规则版本 }}</td>
            <td>{{ window.状态 }}</td>
            <td>{{ window.校验批次号 || '—' }}</td>
            <td>{{ window.最近校验 || '—' }}</td>
          </tr>
        </tbody>
      </table>
      <table v-if="latestAudit" class="data-table audit-table" data-testid="latest-audit">
        <tbody>
          <tr><th>校验批次</th><td>{{ latestAudit.批次号 }}</td><th>判定绑定</th><td>{{ latestAudit.阈值版本 }} / 解析规则 {{ latestAudit.解析规则版本 }}</td></tr>
          <tr><th>放行 / 暂缓 / 重复</th><td>{{ latestAudit.放行数 }} / {{ latestAudit.暂缓数 }} / {{ latestAudit.重复丢弃数 }}</td><th>当前解析规则</th><td>{{ latestAudit.当前解析规则版本 }}</td></tr>
          <tr><th>首次关联可用更新</th><td colspan="3">{{ (latestAudit.首次关联可用更新 || []).map((item: any) => `${item.事件编号}@${item.位点}[${item.修改字段.join('、')}]`).join('；') || '无' }}</td></tr>
          <tr><th>暂缓确认后需重算</th><td colspan="3">{{ (latestAudit.暂缓确认后需重算输出 || []).map((item: any) => `${item.输出对象}(${item.受影响范围})`).join('；') || '无' }}</td></tr>
        </tbody>
      </table>
    </section>

    <table class="data-table">
      <thead>
        <tr>
          <th v-for="column in meta.fields" :key="column">{{ column }}</th>
          <th>当前状态</th>
          <th>可执行动作</th>
        </tr>
      </thead>
      <tbody>
        <tr v-for="row in rows" :key="String(row.id)" :data-testid="`row-${row.id}`" :data-status="String(row.status)">
          <td v-for="column in meta.fields" :key="column">{{ row[column] ?? '—' }}</td>
          <td>{{ row.status }}</td>
          <td class="row-actions">
            <button
              v-for="action in meta.actions"
              :key="action"
              class="link"
              type="button"
              :data-testid="`action-${row.id}-${action}`"
              @click="runAction(action, row)"
            >
              {{ action }}
            </button>
          </td>
        </tr>
        <tr v-if="!rows.length">
          <td :colspan="meta.fields.length + 2" class="empty-state">
            暂无{{ meta.name }}数据，可先登记{{ meta.entity }}
          </td>
        </tr>
      </tbody>
    </table>

    <div v-if="showCreate" class="modal-backdrop" data-testid="create-modal">
      <form class="modal" @submit.prevent="submitCreate">
        <h3>登记{{ meta.entity }}</h3>
        <label v-for="field in meta.fields" :key="field" class="create-field">
          <span>{{ field }}</span>
          <input v-model="createValues[field]" :data-testid="`create-${field}`" required />
        </label>
        <div class="page-actions">
          <button class="btn" type="button" @click="showCreate = false">取消</button>
          <button class="btn primary" data-testid="submit-create" type="submit" :disabled="creating">
            {{ creating ? '提交中…' : '确认登记' }}
          </button>
        </div>
      </form>
    </div>

    <footer class="page-foot">
      <span>共 {{ total }} 条{{ meta.name }}记录</span>
      <span v-if="checkMessage" class="check-message">{{ checkMessage }}</span>
      <span v-if="errorMessage" class="error-text">{{ errorMessage }}</span>
    </footer>
  </section>
</template>

<script setup lang="ts">
import { computed, onMounted, ref } from 'vue'

import {
  createEntry,
  downloadEntries,
  listCheckAudits,
  listCheckWindows,
  listEntries,
  moduleMeta,
  runAction as applyAction,
  runCheckWindow,
} from '@/api/backend-service'
import type { EntryRow } from '@/data/types'

const props = defineProps<{ moduleKey: string }>()
const meta = moduleMeta(props.moduleKey)
const rows = ref<EntryRow[]>([])
const total = ref(0)
const errorMessage = ref('')
const filters = ref<Record<string, string>>({})
const filterFields = meta.fields.slice(0, 3)
const showCreate = ref(false)
const creating = ref(false)
const createValues = ref<Record<string, string>>({})
const windows = ref<Record<string, any>[]>([])
const selectedWindow = ref('')
const latestAudit = ref<Record<string, any> | null>(null)
const runningWindow = ref(false)
const checkMessage = ref('')
const stats = computed(() => [
  { label: meta.metrics[0], value: total.value },
  { label: meta.metrics[1], value: rows.value.filter((row) => row.status === meta.statuses[1]).length },
  {
    label: meta.metrics[2],
    value: rows.value.filter((row) => row.status === meta.statuses[meta.statuses.length - 1]).length,
  },
])
const statusSummary = computed(() =>
  meta.statuses.map((status) => ({
    status,
    count: rows.value.filter((row) => String(row.status) === status).length,
  })),
)

function resetFilters() {
  filters.value = {}
  void reload()
}

async function exportRows() {
  errorMessage.value = ''
  try {
    await downloadEntries(meta.key, filters.value)
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '导出失败'
  }
}

function openCreate() {
  createValues.value = Object.fromEntries(meta.fields.map((field) => [field, '']))
  errorMessage.value = ''
  showCreate.value = true
}

async function submitCreate() {
  creating.value = true
  errorMessage.value = ''
  try {
    const result = await createEntry(meta.key, createValues.value)
    if (!result.ok) throw new Error(result.message)
    showCreate.value = false
    await reload()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '登记失败'
  } finally {
    creating.value = false
  }
}

async function runAction(action: string, row: EntryRow) {
  errorMessage.value = ''
  try {
    const result = await applyAction(meta.key, Number(row.id), action)
    if (!result.ok) throw new Error(result.message)
    await reload()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '操作失败'
  }
}

async function reload() {
  errorMessage.value = ''
  try {
    const payload = await listEntries(meta.key, filters.value)
    rows.value = payload.items
    total.value = payload.total
    if (meta.key === 'key_check') await reloadWindows()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '业务列表读取失败'
  }
}

async function reloadWindows() {
  try {
    const payload = await listCheckWindows()
    windows.value = payload.items as Record<string, any>[]
    if (!selectedWindow.value && windows.value.length) {
      selectedWindow.value = String(windows.value[0].窗口编号)
    }
    if (selectedWindow.value) await reloadAudit()
  } catch {
    windows.value = []
  }
}

async function reloadAudit() {
  try {
    const payload = await listCheckAudits(selectedWindow.value)
    latestAudit.value = payload.items.length ? (payload.items[payload.items.length - 1] as Record<string, any>) : null
  } catch {
    latestAudit.value = null
  }
}

function selectWindow(windowId: string) {
  selectedWindow.value = windowId
  checkMessage.value = ''
  void reloadAudit()
}

async function runWindow() {
  if (!selectedWindow.value) return
  runningWindow.value = true
  errorMessage.value = ''
  try {
    const result = await runCheckWindow(selectedWindow.value)
    checkMessage.value = result.message
    if (!result.ok) throw new Error(result.message)
    await reload()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '窗口校验失败'
  } finally {
    runningWindow.value = false
  }
}

onMounted(reload)
</script>
