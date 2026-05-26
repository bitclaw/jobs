import { describe, expect, test } from 'bun:test';
import { cronMatches, nextCronOccurrence, parseCron } from './cron';

describe('parseCron', () => {
  test('parses * * * * * (every minute)', () => {
    const parsed = parseCron('* * * * *');
    expect(parsed.minutes.size).toBe(60);
    expect(parsed.hours.size).toBe(24);
    expect(parsed.daysOfMonth.size).toBe(31);
    expect(parsed.months.size).toBe(12);
    expect(parsed.daysOfWeek.size).toBe(7);
  });

  test('parses specific values: 30 2 15 6 3', () => {
    const parsed = parseCron('30 2 15 6 3');
    expect(parsed.minutes.has(30)).toBe(true);
    expect(parsed.minutes.size).toBe(1);
    expect(parsed.hours.has(2)).toBe(true);
    expect(parsed.daysOfMonth.has(15)).toBe(true);
    expect(parsed.months.has(6)).toBe(true);
    expect(parsed.daysOfWeek.has(3)).toBe(true);
  });

  test('parses step: */15 * * * *', () => {
    const parsed = parseCron('*/15 * * * *');
    expect([...parsed.minutes].sort((a, b) => a - b)).toEqual([0, 15, 30, 45]);
  });

  test('parses range: 1-5 * * * *', () => {
    const parsed = parseCron('1-5 * * * *');
    expect([...parsed.minutes].sort((a, b) => a - b)).toEqual([1, 2, 3, 4, 5]);
  });

  test('parses range with step: 0-30/10 * * * *', () => {
    const parsed = parseCron('0-30/10 * * * *');
    expect([...parsed.minutes].sort((a, b) => a - b)).toEqual([0, 10, 20, 30]);
  });

  test('parses list: 0,15,30,45 * * * *', () => {
    const parsed = parseCron('0,15,30,45 * * * *');
    expect([...parsed.minutes].sort((a, b) => a - b)).toEqual([0, 15, 30, 45]);
  });

  test('parses Monday at 2am: 0 2 * * 1', () => {
    const parsed = parseCron('0 2 * * 1');
    expect(parsed.minutes.has(0)).toBe(true);
    expect(parsed.hours.has(2)).toBe(true);
    expect(parsed.daysOfWeek.has(1)).toBe(true);
    expect(parsed.daysOfWeek.size).toBe(1);
  });

  test('rejects invalid field count', () => {
    expect(() => parseCron('* * *')).toThrow('expected 5 fields, got 3');
  });

  test('rejects out-of-range values', () => {
    expect(() => parseCron('60 * * * *')).toThrow('Invalid value: 60');
    expect(() => parseCron('* 25 * * *')).toThrow('Invalid value: 25');
    expect(() => parseCron('* * 0 * *')).toThrow('Invalid value: 0');
    expect(() => parseCron('* * * 13 *')).toThrow('Invalid value: 13');
    expect(() => parseCron('* * * * 7')).toThrow('Invalid value: 7');
  });

  test('rejects invalid range', () => {
    expect(() => parseCron('5-2 * * * *')).toThrow('Invalid range');
  });
});

describe('cronMatches', () => {
  test('every minute matches any date', () => {
    const parsed = parseCron('* * * * *');
    expect(cronMatches(parsed, new Date('2025-06-15T10:30:00Z'))).toBe(true);
  });

  test('specific time matches correctly', () => {
    const parsed = parseCron('30 14 * * *');
    expect(cronMatches(parsed, new Date('2025-06-15T14:30:00Z'))).toBe(true);
    expect(cronMatches(parsed, new Date('2025-06-15T14:31:00Z'))).toBe(false);
    expect(cronMatches(parsed, new Date('2025-06-15T15:30:00Z'))).toBe(false);
  });

  test('day of week matches', () => {
    // 2025-06-16 is a Monday (day 1)
    const parsed = parseCron('0 9 * * 1');
    expect(cronMatches(parsed, new Date('2025-06-16T09:00:00Z'))).toBe(true);
    expect(cronMatches(parsed, new Date('2025-06-17T09:00:00Z'))).toBe(false);
  });
});

describe('nextCronOccurrence', () => {
  test('finds next minute for * * * * *', () => {
    const parsed = parseCron('* * * * *');
    const after = new Date('2025-06-15T10:30:00Z');
    const next = nextCronOccurrence(parsed, after);
    expect(next.toISOString()).toBe('2025-06-15T10:31:00.000Z');
  });

  test('finds next occurrence for 0 2 * * 1 (Monday 2am)', () => {
    const parsed = parseCron('0 2 * * 1');
    // 2025-06-15 is a Sunday
    const after = new Date('2025-06-15T10:00:00Z');
    const next = nextCronOccurrence(parsed, after);
    // Next Monday is 2025-06-16
    expect(next.toISOString()).toBe('2025-06-16T02:00:00.000Z');
  });

  test('finds next occurrence for */15', () => {
    const parsed = parseCron('*/15 * * * *');
    const after = new Date('2025-06-15T10:02:00Z');
    const next = nextCronOccurrence(parsed, after);
    expect(next.toISOString()).toBe('2025-06-15T10:15:00.000Z');
  });

  test('wraps to next day when no match today', () => {
    const parsed = parseCron('0 2 * * *');
    const after = new Date('2025-06-15T03:00:00Z');
    const next = nextCronOccurrence(parsed, after);
    expect(next.toISOString()).toBe('2025-06-16T02:00:00.000Z');
  });
});
