import { afterEach, beforeEach, describe, expect, test } from 'bun:test';
import { JobQueue } from './queue';
import { Scheduler } from './scheduler';

type TestJobs = {
  'report:generate': { type: string };
  'data:cleanup': Record<string, never>;
  'email:digest': Record<string, never>;
};

describe('Scheduler', () => {
  let queue: JobQueue<TestJobs>;
  let scheduler: Scheduler<TestJobs>;

  beforeEach(() => {
    queue = new JobQueue<TestJobs>(':memory:');
    scheduler = new Scheduler(queue);
  });

  afterEach(() => {
    scheduler.stop();
    queue.close();
  });

  describe('register', () => {
    test('creates a new schedule', () => {
      const schedule = scheduler.register(
        'daily-report',
        'report:generate',
        '0 2 * * *',
        { data: { type: 'daily' } }
      );

      expect(schedule.name).toBe('daily-report');
      expect(schedule.type).toBe('report:generate');
      expect(schedule.cron).toBe('0 2 * * *');
      expect(schedule.data).toEqual({ type: 'daily' });
      expect(schedule.enabled).toBe(true);
      expect(schedule.overlap).toBe(false);
      expect(schedule.nextRunAt).not.toBeNull();
    });

    test('upsert updates cron and recalculates next_run_at', () => {
      // Use crons that produce different next times: 0 3 * * * (3am) vs 0 14 * * * (2pm)
      scheduler.register('cleanup', 'data:cleanup', '0 3 * * *');
      const original = scheduler.getSchedule('cleanup')!;

      scheduler.register('cleanup', 'data:cleanup', '0 14 * * *');
      const updated = scheduler.getSchedule('cleanup')!;

      expect(updated.cron).toBe('0 14 * * *');
      // next_run_at should be recalculated since cron changed
      expect(updated.nextRunAt).not.toBe(original.nextRunAt);
    });

    test('upsert preserves enabled state (runtime state)', () => {
      scheduler.register('cleanup', 'data:cleanup', '0 * * * *');
      scheduler.pauseSchedule('cleanup');

      expect(scheduler.getSchedule('cleanup')!.enabled).toBe(false);

      // Re-register should NOT re-enable
      scheduler.register('cleanup', 'data:cleanup', '0 * * * *');
      expect(scheduler.getSchedule('cleanup')!.enabled).toBe(false);
    });

    test('upsert preserves next_run_at when cron unchanged', () => {
      scheduler.register('cleanup', 'data:cleanup', '0 * * * *');
      const first = scheduler.getSchedule('cleanup')!;

      // Re-register with same cron , next_run_at should be preserved
      scheduler.register('cleanup', 'data:cleanup', '0 * * * *', {
        data: {},
        maxRetries: 5
      });
      const second = scheduler.getSchedule('cleanup')!;

      expect(second.nextRunAt).toBe(first.nextRunAt);
      expect(second.maxRetries).toBe(5);
    });

    test('updates type and data on re-register', () => {
      scheduler.register('job', 'report:generate', '0 2 * * *', {
        data: { type: 'daily' }
      });
      scheduler.register('job', 'data:cleanup', '0 2 * * *');

      const schedule = scheduler.getSchedule('job')!;
      expect(schedule.type).toBe('data:cleanup');
      expect(schedule.data).toEqual({});
    });
  });

  describe('cleanup', () => {
    test('removes orphan schedules not in registered set', () => {
      scheduler.register('keep-1', 'data:cleanup', '0 * * * *');
      scheduler.register('keep-2', 'email:digest', '0 9 * * 1');
      scheduler.register('remove-me', 'report:generate', '0 2 * * *');

      const removed = scheduler.cleanup(['keep-1', 'keep-2']);
      expect(removed).toBe(1);

      expect(scheduler.getSchedule('keep-1')).not.toBeNull();
      expect(scheduler.getSchedule('keep-2')).not.toBeNull();
      expect(scheduler.getSchedule('remove-me')).toBeNull();
    });

    test('removes all schedules when empty set provided', () => {
      scheduler.register('a', 'data:cleanup', '0 * * * *');
      scheduler.register('b', 'email:digest', '0 * * * *');

      const removed = scheduler.cleanup([]);
      expect(removed).toBe(2);
      expect(scheduler.getSchedules()).toHaveLength(0);
    });
  });

  describe('pause/resume', () => {
    test('pauses and resumes a schedule', () => {
      scheduler.register('cleanup', 'data:cleanup', '0 * * * *');

      scheduler.pauseSchedule('cleanup');
      expect(scheduler.getSchedule('cleanup')!.enabled).toBe(false);

      scheduler.resumeSchedule('cleanup');
      expect(scheduler.getSchedule('cleanup')!.enabled).toBe(true);
    });

    test('pause persists across register (runtime state preserved)', () => {
      scheduler.register('cleanup', 'data:cleanup', '0 * * * *');
      scheduler.pauseSchedule('cleanup');

      scheduler.register('cleanup', 'data:cleanup', '0 * * * *');
      expect(scheduler.getSchedule('cleanup')!.enabled).toBe(false);
    });
  });

  describe('getSchedules / getSchedule', () => {
    test('lists all schedules', () => {
      scheduler.register('a', 'data:cleanup', '0 * * * *');
      scheduler.register('b', 'email:digest', '0 9 * * 1');

      const schedules = scheduler.getSchedules();
      expect(schedules).toHaveLength(2);
    });

    test('returns null for non-existent schedule', () => {
      expect(scheduler.getSchedule('nope')).toBeNull();
    });
  });

  describe('removeSchedule', () => {
    test('removes a schedule', () => {
      scheduler.register('cleanup', 'data:cleanup', '0 * * * *');
      expect(scheduler.removeSchedule('cleanup')).toBe(true);
      expect(scheduler.getSchedule('cleanup')).toBeNull();
    });

    test('returns false for non-existent schedule', () => {
      expect(scheduler.removeSchedule('nope')).toBe(false);
    });
  });

  describe('tick', () => {
    test('enqueues jobs for due schedules', () => {
      scheduler.register('cleanup', 'data:cleanup', '* * * * *');

      // Force next_run_at to the past so tick() picks it up
      queue.db
        .query("UPDATE schedules SET next_run_at = '2020-01-01T00:00:00.000Z'")
        .run();

      const enqueued = scheduler.tick();
      expect(enqueued).toBe(1);

      const job = queue.pollAndClaim('data:cleanup');
      expect(job).not.toBeNull();
    });

    test('does not enqueue future schedules', () => {
      scheduler.register('cleanup', 'data:cleanup', '* * * * *');

      // Force next_run_at far in the future
      queue.db
        .query("UPDATE schedules SET next_run_at = '2099-01-01T00:00:00.000Z'")
        .run();

      const enqueued = scheduler.tick();
      expect(enqueued).toBe(0);
    });

    test('does not enqueue disabled schedules', () => {
      scheduler.register('cleanup', 'data:cleanup', '* * * * *');
      scheduler.pauseSchedule('cleanup');

      queue.db
        .query("UPDATE schedules SET next_run_at = '2020-01-01T00:00:00.000Z'")
        .run();

      const enqueued = scheduler.tick();
      expect(enqueued).toBe(0);
    });

    test('skips overlap=false when active job exists', () => {
      scheduler.register('cleanup', 'data:cleanup', '* * * * *', {
        overlap: false
      });

      // Force due
      queue.db
        .query("UPDATE schedules SET next_run_at = '2020-01-01T00:00:00.000Z'")
        .run();

      // Create an active job of the same type
      queue.add('data:cleanup', {} as TestJobs['data:cleanup']);

      const enqueued = scheduler.tick();
      expect(enqueued).toBe(0);
    });

    test('allows overlap=true when active job exists', () => {
      scheduler.register('cleanup', 'data:cleanup', '* * * * *', {
        overlap: true
      });

      // Force due
      queue.db
        .query("UPDATE schedules SET next_run_at = '2020-01-01T00:00:00.000Z'")
        .run();

      // Create an active job of the same type
      queue.add('data:cleanup', {} as TestJobs['data:cleanup']);

      const enqueued = scheduler.tick();
      expect(enqueued).toBe(1);
    });

    test('updates last_run_at and next_run_at after tick', () => {
      scheduler.register('cleanup', 'data:cleanup', '* * * * *');

      queue.db
        .query("UPDATE schedules SET next_run_at = '2020-01-01T00:00:00.000Z'")
        .run();

      scheduler.tick();

      const schedule = scheduler.getSchedule('cleanup')!;
      expect(schedule.lastRunAt).not.toBeNull();
      expect(schedule.nextRunAt).not.toBeNull();
      // next_run_at should be in the future
      expect(new Date(schedule.nextRunAt!).getTime()).toBeGreaterThan(
        Date.now() - 60_000
      );
    });

    test('enqueues multiple due schedules', () => {
      scheduler.register('a', 'data:cleanup', '* * * * *');
      scheduler.register('b', 'email:digest', '* * * * *');

      queue.db
        .query("UPDATE schedules SET next_run_at = '2020-01-01T00:00:00.000Z'")
        .run();

      const enqueued = scheduler.tick();
      expect(enqueued).toBe(2);
    });
  });

  describe('start/stop', () => {
    test('start creates interval, stop clears it', () => {
      scheduler.start(60_000);
      // Calling start again is a no-op
      scheduler.start(60_000);

      scheduler.stop();
      // Calling stop again is a no-op
      scheduler.stop();
    });
  });
});
