import {
  decomposeTask,
  type DecompositionResult,
} from './decomposer.js';
import { buildBatches, validateDependencies } from './dependency.js';
import {
  matchAgents,
  generateMatchSummary,
  type AgentMatcherConfig,
} from './matcher.js';
import { synthesizeResults, detectConflicts } from './synthesizer.js';
import type {
  OrchestrateParams,
  OrchestrateResult,
  SubtaskResult,
  Batch,
  OrchestrationConfig,
  Subtask,
} from './types.js';

/**
 * Spawn Subtask 函数类型
 * 由调用者提供，负责实际启动子代理并等待完成
 */
export type SpawnSubtaskFn = (
  subtask: Subtask,
  agentId: string,
  timeout: number,
) => Promise<SubtaskResult>;

/**
 * Orchestrator Skill 主入口
 *
 * 流程：
 * 1. Decompose: 将任务拆分为原子子任务
 * 2. Validate: 验证依赖（拓扑排序）
 * 3. Match: 为每个子任务匹配agent
 * 4. Spawn: 按批次并行spawn子代理（通过spawnSubtask回调）
 * 5. Wait: 等待所有子任务完成
 * 6. Synthesize: 收集结果并合成最终报告
 *
 * @param params 包含任务、配置、进度回调，以及必需的spawnSubtask函数
 */
export async function orchestrate(params: {
  task: string;
  config?: OrchestrationConfig;
  onProgress?: (batch: number, total: number, completed: number) => void;
  /** 实际启动子代理的实现 */
  spawnSubtask: SpawnSubtaskFn;
}): Promise<OrchestrateResult> {
  const {
    task,
    config = {},
    onProgress,
    spawnSubtask,
  } = params;

  const results: SubtaskResult[] = [];

  try {
    // === 阶段1: 任务分解 ===
    console.log('[orchestrator] Decomposing task...');
    const decomposition: DecompositionResult = await decomposeTask(
      task,
      config.decomposePrompt,
    );

    if (decomposition.subtasks.length === 0) {
      return {
        status: 'failed',
        totalTasks: 0,
        completedTasks: 0,
        failedTasks: 0,
        batches: 0,
        result: '任务分解失败：未生成任何子任务',
        results: [],
      };
    }

    console.log(`[orchestrator] Decomposed into ${decomposition.subtasks.length} subtasks`);

    // === 阶段2: 依赖验证 ===
    console.log('[orchestrator] Validating dependencies...');
    const validation = validateDependencies(decomposition.subtasks);
    if (!validation.valid) {
      return {
        status: 'failed',
        totalTasks: decomposition.subtasks.length,
        completedTasks: 0,
        failedTasks: decomposition.subtasks.length,
        batches: 0,
        result: `依赖验证失败: ${validation.cycles?.join(', ')}`,
        results: [],
      };
    }

    // === 阶段3: 构建批处理 ===
    console.log('[orchestrator] Building batches...');
    const batches: Batch[] = buildBatches(decomposition.subtasks);
    const totalBatches = batches.length;
    console.log(`[orchestrator] Created ${totalBatches} batches (parallel depth)`);

    // === 阶段4: Agent匹配 ===
    console.log('[orchestrator] Matching agents...');
    const agentConfig: AgentMatcherConfig = {
      agentMap: config.agentMap ?? getDefaultAgentMap(),
      defaultAgent: config.defaultAgent ?? resolveDefaultAgent(),
    };

    const matches = matchAgents(decomposition.subtasks, agentConfig);
    console.log(generateMatchSummary(decomposition.subtasks, matches));

    // === 阶段5: 按批次执行 ===
    console.log('[orchestrator] Spawning subagents...');
    const subtaskIdToResult = new Map<string, SubtaskResult>();

    for (let batchIdx = 0; batchIdx < batches.length; batchIdx++) {
      const batch = batches[batchIdx];
      console.log(`[orchestrator] Batch ${batchIdx + 1}/${totalBatches}: ${batch.length} tasks`);

      // 并发控制：限制每批次并发数
      const maxConcurrent = config.maxConcurrent ?? getDefaultMaxConcurrent();

      // 并行任务执行函数
      const executeSubtask = async (subtaskId: string): Promise<void> => {
        const subtask = decomposition.subtasks.find(st => st.id === subtaskId)!;
        const match = matches.get(subtaskId)!;
        try {
          const result = await spawnSubtask(subtask, match.agentId, config.timeoutPerTask ?? getDefaultTimeout());
          subtaskIdToResult.set(subtaskId, result);
        } catch (error: any) {
          subtaskIdToResult.set(subtaskId, {
            subtaskId,
            status: 'failed',
            output: undefined,
            error: error.message,
            durationMs: 0,
          });
        }
      };

      // 简单实现：批量并行执行（依赖外部maxSpawnDepth控制总深度）
      const batchPromises = batch.map(subtaskId => executeSubtask(subtaskId));

      // 如果批次超过并发限制，分批等待
      if (batch.length > maxConcurrent) {
        for (let i = 0; i < batch.length; i += maxConcurrent) {
          const chunk = batchPromises.slice(i, i + maxConcurrent);
          await Promise.all(chunk);
        }
      } else {
        await Promise.all(batchPromises);
      }

      // 进度回调
      if (onProgress) {
        const completedSoFar = Array.from(subtaskIdToResult.values()).filter(r => r.status === 'completed').length;
        onProgress({
          phase: 'batch-complete',
          batch: batchIdx + 1,
          totalBatches,
          completed: completedSoFar,
          total: decomposition.subtasks.length,
        });
      }
    }

    // === 阶段6: 收集全部结果 ===
    results.push(...decomposition.subtasks.map(st => {
      const existing = subtaskIdToResult.get(st.id);
      return existing ?? {
        subtaskId: st.id,
        status: 'skipped',
        output: undefined,
        error: '未执行',
        durationMs: 0,
      };
    }));

    // === 阶段7: 冲突检测 ===
    console.log('[orchestrator] Detecting conflicts...');
    const conflicts = detectConflicts(results);

    // === 阶段8: 结果合成 ===
    console.log('[orchestrator] Synthesizing final result...');
    const final = synthesizeResults(results, conflicts);
    final.batches = totalBatches;

    return final;

  } catch (error: any) {
    console.error('[orchestrator] Fatal error:', error);
    return {
      status: 'failed',
      totalTasks: results.length,
      completedTasks: results.filter(r => r.status === 'completed').length,
      failedTasks: results.length,
      batches: 0,
      result: `Orchestration fatal error: ${error.message}`,
      results,
    };
  }
}

// === 配置帮助函数 ===

function getDefaultAgentMap(): Record<string, string> {
  // 默认映射：type → agentId
  return {
    code: 'coding-agent',
    test: 'testing-agent',
    docs: 'docs-writer',
    research: 'researcher',
    review: 'code-reviewer',
    refactor: 'coding-agent',
    config: 'coding-agent',
  };
}

function resolveDefaultAgent(): string {
  // 默认回退到主agent
  return 'agent:default:main'; // 或读取config
}

function getDefaultMaxConcurrent(): number {
  return 8; // 与Openspawn默认一致
}

function getDefaultTimeout(): number {
  return 900; // 15分钟
}
