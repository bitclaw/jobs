// packages/jobs/src/scheduler.ts
// Cron scheduler — hybrid register/upsert pattern for recurring jobs
import { nextCronOccurrence, parseCron } from './cron';
import { nowISO } from './utils';
function toSchedule(row) {
    return {
        id: row.id,
        name: row.name,
        type: row.type,
        data: JSON.parse(row.data),
        cron: row.cron,
        timezone: row.timezone,
        enabled: row.enabled === 1,
        overlap: row.overlap === 1,
        maxRetries: row.max_retries,
        lastRunAt: row.last_run_at,
        nextRunAt: row.next_run_at,
        createdAt: row.created_at,
        updatedAt: row.updated_at
    };
}
export class Scheduler {
    queue;
    timer = null;
    upsertStmt;
    selectByNameStmt;
    selectAllStmt;
    selectDueStmt;
    updateLastRunStmt;
    pauseStmt;
    resumeStmt;
    removeStmt;
    constructor(queue) {
        this.queue = queue;
        const db = queue.db;
        this.upsertStmt = db.query(`
      INSERT INTO schedules (name, type, data, cron, timezone, overlap, max_retries, next_run_at, created_at, updated_at)
      VALUES ($name, $type, $data, $cron, $timezone, $overlap, $maxRetries, $nextRunAt, $now, $now)
      ON CONFLICT(name) DO UPDATE SET
        type = $type,
        data = $data,
        cron = $cron,
        timezone = $timezone,
        overlap = $overlap,
        max_retries = $maxRetries,
        next_run_at = CASE WHEN schedules.cron != $cron THEN $nextRunAt ELSE schedules.next_run_at END,
        updated_at = $now
    `);
        this.selectByNameStmt = db.query('SELECT * FROM schedules WHERE name = $name');
        this.selectAllStmt = db.query('SELECT * FROM schedules ORDER BY name');
        this.selectDueStmt = db.query(`
      SELECT * FROM schedules
      WHERE enabled = 1 AND next_run_at IS NOT NULL AND next_run_at <= $now
    `);
        this.updateLastRunStmt = db.query(`
      UPDATE schedules
      SET last_run_at = $now, next_run_at = $nextRunAt, updated_at = $now
      WHERE id = $id
    `);
        this.pauseStmt = db.query(`
      UPDATE schedules SET enabled = 0, updated_at = $now WHERE name = $name
    `);
        this.resumeStmt = db.query(`
      UPDATE schedules SET enabled = 1, updated_at = $now WHERE name = $name
    `);
        this.removeStmt = db.query('DELETE FROM schedules WHERE name = $name');
    }
    register(name, type, cron, options) {
        const parsed = parseCron(cron);
        const nextRunAt = nextCronOccurrence(parsed, new Date()).toISOString();
        const now = nowISO();
        this.upsertStmt.run({
            $name: name,
            $type: type,
            $data: JSON.stringify(options?.data ?? {}),
            $cron: cron,
            $timezone: options?.timezone ?? 'UTC',
            $overlap: options?.overlap ? 1 : 0,
            $maxRetries: options?.maxRetries ?? 3,
            $nextRunAt: nextRunAt,
            $now: now
        });
        return this.getSchedule(name);
    }
    cleanup(registeredNames) {
        if (registeredNames.length === 0) {
            const result = this.queue.db.query('DELETE FROM schedules').run();
            return result.changes;
        }
        // Build parameterized query for the IN clause
        const placeholders = registeredNames.map((_, i) => `$p${i}`).join(', ');
        const params = {};
        for (let i = 0; i < registeredNames.length; i++) {
            params[`$p${i}`] = registeredNames[i];
        }
        const result = this.queue.db
            .query(`DELETE FROM schedules WHERE name NOT IN (${placeholders})`)
            .run(params);
        return result.changes;
    }
    pauseSchedule(name) {
        this.pauseStmt.run({ $name: name, $now: nowISO() });
    }
    resumeSchedule(name) {
        this.resumeStmt.run({ $name: name, $now: nowISO() });
    }
    getSchedules() {
        const rows = this.selectAllStmt.all();
        return rows.map(toSchedule);
    }
    getSchedule(name) {
        const row = this.selectByNameStmt.get({
            $name: name
        });
        return row ? toSchedule(row) : null;
    }
    removeSchedule(name) {
        const result = this.removeStmt.run({ $name: name });
        return result.changes > 0;
    }
    tick() {
        const now = nowISO();
        const dueSchedules = this.selectDueStmt.all({ $now: now });
        let enqueued = 0;
        for (const row of dueSchedules) {
            // Overlap check: if overlap=false, skip if there's a pending/processing job of this type
            if (row.overlap === 0) {
                const active = this.queue.db
                    .query("SELECT 1 FROM jobs WHERE type = $type AND status IN ('pending', 'processing') LIMIT 1")
                    .get({ $type: row.type });
                if (active) {
                    // Skip this tick, recalculate next_run_at
                    const parsed = parseCron(row.cron);
                    const nextRunAt = nextCronOccurrence(parsed, new Date()).toISOString();
                    this.updateLastRunStmt.run({
                        $id: row.id,
                        $now: now,
                        $nextRunAt: nextRunAt
                    });
                    continue;
                }
            }
            // Enqueue the job
            this.queue.db
                .query(`INSERT INTO jobs (type, data, status, priority, max_retries, run_at, batch_id)
           VALUES ($type, $data, 'pending', 0, $maxRetries, $now, NULL)`)
                .run({
                $type: row.type,
                $data: row.data,
                $maxRetries: row.max_retries,
                $now: now
            });
            // Update schedule timestamps
            const parsed = parseCron(row.cron);
            const nextRunAt = nextCronOccurrence(parsed, new Date()).toISOString();
            this.updateLastRunStmt.run({
                $id: row.id,
                $now: now,
                $nextRunAt: nextRunAt
            });
            enqueued++;
        }
        return enqueued;
    }
    start(intervalMs = 60_000) {
        if (this.timer)
            return;
        this.timer = setInterval(() => this.tick(), intervalMs);
    }
    stop() {
        if (this.timer) {
            clearInterval(this.timer);
            this.timer = null;
        }
    }
}
