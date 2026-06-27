import type { JobQueue } from './queue';
import type { AddJobOptions, BackoffConfig, JobMap, WorkflowExecution, WorkflowExecutionStatus, WorkflowReconcileResult, WorkflowRunResult } from './types';
export type { WorkflowExecution, WorkflowExecutionStatus, WorkflowReconcileResult, WorkflowRunResult };
type StepStepOptions = Omit<AddJobOptions, 'dependsOn'> & {
    dependsOn?: string[];
    backoff?: BackoffConfig;
};
export declare class WorkflowBuilder<TMap extends JobMap, TStepNames extends string = never> {
    private readonly queue;
    private readonly name;
    private readonly specs;
    private readonly compensations;
    constructor(queue: JobQueue<TMap>, name: string);
    step<SName extends string, K extends string & keyof TMap>(stepName: SName, jobType: K, data: TMap[K], options?: StepStepOptions): WorkflowBuilder<TMap, TStepNames | SName>;
    onFail<K extends string & keyof TMap>(stepName: TStepNames, compensation: {
        compensate: K;
        compensateData: TMap[K];
    }): this;
    run(instanceId?: string): WorkflowRunResult<TStepNames>;
}
export declare class WorkflowEngine<TMap extends JobMap> {
    private readonly queue;
    constructor(queue: JobQueue<TMap>);
    workflow<TStepNames extends string = never>(name: string): WorkflowBuilder<TMap, TStepNames>;
    getExecution(instanceId: string): WorkflowExecution | null;
    getStepResult<T>(instanceId: string, stepName: string): T | null;
    listExecutions(opts?: {
        status?: WorkflowExecutionStatus;
        name?: string;
        limit?: number;
        offset?: number;
    }): WorkflowExecution[];
    reconcile(): WorkflowReconcileResult;
    private reconcileExecution;
}
//# sourceMappingURL=workflow.d.ts.map