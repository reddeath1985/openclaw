import { Type } from "@sinclair/typebox";
import { loadConfig } from "../../config/config.js";
import { getSubagentDepthFromSessionStore } from "../../agents/subagent-depth.js";
import { resolveDefaultAgentId } from "../../agents/agent-scope.js";
import type { OpenClawConfig } from "../../config/config.js";
import type { AnyAgentTool } from "../common.js";
import { readStringParam } from "../common.js";
import { resolveMainSessionAlias } from "../sessions-helpers.js";
import {
  type OrchestrateParams,
  type OrchestrateResult,
  type Subtask,
  type SubtaskResult,
} from "../../skills/orchestrator/scripts/types.js";
import { orchestrate, type SpawnSubtaskFn } from "../../skills/orchestrator/scripts/orchestrator.js";
import { spawnSubagentDirect } from "../../agents/subagent-spawn.js";
import { callGateway } from "../../gateway/call.js";

const OrchestrateToolSchema = Type.Object({
  task: Type.String({ description: "要执行的高级任务描述" }),
  agentMap: Type.Optional(
    Type.Record(Type.String(), Type.String()),
  ),
  maxConcurrent: Type.Optional(Type.Number()),
  timeoutPerTask: Type.Optional(Type.Number()),
  decomposePrompt: Type.Optional(Type.String()),
});

/**
 * 实现spawnSubtask：调用子代理并等待完成
 */
async function spawnSubtaskImpl(
  subtask: Subtask,
  agentId: string,
  timeoutSeconds: number,
  opts: {
    agentSessionKey: string;
    agentChannel?: string;
    agentAccountId?: string;
    agentTo?: string;
    agentThreadId?: string | number;
    agentGroupId?: string | null;
    agentGroupChannel?: string | null;
    agentGroupSpace?: string | null;
    workspaceDir?: string;
    config?: OpenClawConfig;
    requesterAgentIdOverride?: string;
  },
): Promise<SubtaskResult> {
  const startTime = Date.now();

  // 1. Spawn子代理
  const spawnResult = await spawnSubagentDirect(
    {
      task: subtask.description,
      label: `orchestrate:${subtask.id}`,
      agentId,
      mode: "run", // 一次性执行
      runTimeoutSeconds: timeoutSeconds,
      expectsCompletionMessage: true,
    },
    {
      agentSessionKey: opts.agentSessionKey,
      agentChannel: opts.agentChannel,
      agentAccountId: opts.agentAccountId,
      agentTo: opts.agentTo,
      agentThreadId: opts.agentThreadId,
      requesterAgentIdOverride: opts.requesterAgentIdOverride,
    },
  );

  if (spawnResult.status !== "accepted" || !spawnResult.childSessionKey) {
    throw new Error(spawnResult.error ?? `sessions.spawn failed: ${spawnResult.status}`);
  }

  const childSessionKey = spawnResult.childSessionKey;

  // 2. 等待子代理完成（轮询transcript）
  const deadline = Date.now() + timeoutSeconds * 1000;
  let completed = false;
  let finalOutput = "";
  let errorMsg: string | undefined;

  while (Date.now() < deadline) {
    try {
      // 读取子代理历史
      const history = await callGateway<{ messages: Array<{ role: string; content?: { text?: string }; toolCalls?: any[] }> }>({
        method: "sessions.history",
        params: {
          sessionKey: childSessionKey,
          limit: 100,
          includeTools: true,
        },
        timeoutMs: 10_000,
      });

      if (history.messages.length > 0) {
        // 检查是否完成：最后一条非tool消息是assistant且不包含toolCalls
        let lastNonToolMsg: any = null;
        for (let i = history.messages.length - 1; i >= 0; i--) {
          const msg = history.messages[i];
          if (msg.role === "tool") continue;
          lastNonToolMsg = msg;
          break;
        }

        if (lastNonToolMsg && lastNonToolMsg.role === "assistant") {
          if (!lastNonToolMsg.toolCalls || lastNonToolMsg.toolCalls.length === 0) {
            // 找到了最终回复
            completed = true;
            finalOutput = lastNonToolMsg.content?.text ?? "";
            break;
          }
        }
      }
    } catch (e) {
      // 忽略轮询过程中的错误
    }

    // 2秒间隔
    await new Promise(resolve => setTimeout(resolve, 2000));
  }

  if (!completed) {
    throw new Error(`Timeout waiting for subagent ${childSessionKey} to complete`);
  }

  const durationMs = Date.now() - startTime;

  return {
    subtaskId: subtask.id,
    status: "completed",
    output: finalOutput,
    durationMs,
    sessionKey: childSessionKey,
  };
}

/**
 * 创建orchestrate工具
 * 该工具将复杂任务分解、并行执行子代理、并合成最终结果
 */
export function createOrchestrateTool(opts?: {
  agentSessionKey?: string;
  agentChannel?: string;
  agentAccountId?: string;
  agentTo?: string;
  agentThreadId?: string | number;
  agentGroupId?: string;
  agentGroupChannel?: string;
  agentGroupSpace?: string;
  workspaceDir?: string;
  requesterAgentIdOverride?: string;
  config?: OpenClawConfig;
}): AnyAgentTool {
  return {
    label: "Orchestrate",
    name: "orchestrate",
    description:
      "自动分解复杂任务为原子子任务，并行执行子代理，并合成最终结果。需要 maxSpawnDepth >= 2。",
    parameters: OrchestrateToolSchema,
    execute: async (_toolCallId, args) => {
      const params = args as OrchestrateParams;

      // 权限检查：确保当前会话允许嵌套spawn（maxSpawnDepth >= 2）
      const cfg = opts?.config ?? loadConfig();
      const sessionKey = opts?.agentSessionKey;
      if (!sessionKey) {
        throw new Error("agentSessionKey required for orchestrate");
      }

      const depth = getSubagentDepthFromSessionStore(sessionKey, { cfg });
      const maxDepth = cfg.agents?.defaults?.subagents?.maxSpawnDepth ?? 1;
      if (depth >= maxDepth) {
        throw new Error(
          `orchestrate requires maxSpawnDepth >= 2. Current depth: ${depth}, max: ${maxDepth}. 请检查配置 agents.defaults.subagents.maxSpawnDepth`,
        );
      }

      // 准备orchestration配置
      const orchestrationConfig = {
        agentMap: params.agentMap,
        maxConcurrent: params.maxConcurrent,
        timeoutPerTask: params.timeoutPerTask,
        decomposePrompt: params.decomposePrompt,
      };

      // 执行orchestrate
      try {
        const result: OrchestrateResult = await orchestrate({
          task: params.task,
          config: orchestrationConfig,
          onProgress: (batch, total, completed) => {
            console.log(`[orchestrate] Progress: batch ${batch}/${total}, completed ${completed}`);
          },
          spawnSubtask: (subtask, agentId, timeout) =>
            spawnSubtaskImpl(subtask, agentId, timeout, {
              agentSessionKey: sessionKey,
              agentChannel: opts?.agentChannel,
              agentAccountId: opts?.agentAccountId,
              agentTo: opts?.agentTo,
              agentThreadId: opts?.agentThreadId,
              agentGroupId: opts?.agentGroupId,
              agentGroupChannel: opts?.agentGroupChannel,
              agentGroupSpace: opts?.agentGroupSpace,
              workspaceDir: opts?.workspaceDir,
              config: cfg,
              requesterAgentIdOverride: opts?.requesterAgentIdOverride,
            }),
        });

        // 返回结构化结果
        return {
          content: [{ type: "text", text: formatOrchestrationResult(result) }],
          details: {
            ok: result.status === "completed" || result.status === "partial-failure",
            status: result.status,
            totalTasks: result.totalTasks,
            completedTasks: result.completedTasks,
            failedTasks: result.failedTasks,
            batches: result.batches,
            conflicts: result.conflicts,
            artifacts: result.artifacts,
          },
        };
      } catch (error: any) {
        return {
          content: [{ type: "text", text: `❌ Orchestration failed: ${error.message}` }],
          details: { ok: false, error: error.message },
        };
      }
    },
  };
}

/**
 * 格式化orchestration结果为可读文本
 */
function formatOrchestrationResult(result: OrchestrateResult): string {
  const lines: string[] = [];
  lines.push("=== Orchestration Result ===");
  lines.push(`状态: ${result.status}`);
  lines.push(`任务: ${result.completedTasks}/${result.totalTasks} 完成 (${result.failedTasks} 失败)`);
  lines.push(`批次: ${result.batches}`);

  if (result.conflicts && result.conflicts.length > 0) {
    lines.push(`冲突: ${result.conflicts.length} 个文件需要手动处理`);
    result.conflicts.forEach(c => {
      lines.push(`  - ${c.file} (${c.tasks.join(', ')})`);
    });
  }

  if (result.artifacts && result.artifacts.length > 0) {
    lines.push(`生产物:`);
    result.artifacts.forEach(a => {
      lines.push(`  - [${a.type}] ${a.path} (by ${a.producedBy})`);
    });
  }

  lines.push("\n详细输出:");
  lines.push(result.result);

  return lines.join("\n");
}
