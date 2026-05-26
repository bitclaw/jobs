// packages/jobs/src/cron.ts
// Minimal 5-field cron parser (minute, hour, day-of-month, month, day-of-week)
const FIELD_RANGES = [
    [0, 59], // minute
    [0, 23], // hour
    [1, 31], // day of month
    [1, 12], // month
    [0, 6] // day of week (0=Sun)
];
function parseField(field, min, max) {
    const values = new Set();
    for (const part of field.split(',')) {
        if (part === '*') {
            for (let i = min; i <= max; i++)
                values.add(i);
            continue;
        }
        // */N step from min
        const fullStepMatch = part.match(/^\*\/(\d+)$/);
        if (fullStepMatch) {
            const step = Number(fullStepMatch[1]);
            if (step === 0)
                throw new Error(`Invalid step value: ${part}`);
            for (let i = min; i <= max; i += step)
                values.add(i);
            continue;
        }
        // N-M or N-M/S range with optional step
        const rangeMatch = part.match(/^(\d+)-(\d+)(?:\/(\d+))?$/);
        if (rangeMatch) {
            const start = Number(rangeMatch[1]);
            const end = Number(rangeMatch[2]);
            const step = rangeMatch[3] ? Number(rangeMatch[3]) : 1;
            if (start < min || end > max || start > end) {
                throw new Error(`Invalid range: ${part} (valid: ${min}-${max})`);
            }
            if (step === 0)
                throw new Error(`Invalid step value: ${part}`);
            for (let i = start; i <= end; i += step)
                values.add(i);
            continue;
        }
        // Plain number
        const num = Number(part);
        if (Number.isNaN(num) || num < min || num > max) {
            throw new Error(`Invalid value: ${part} (valid: ${min}-${max})`);
        }
        values.add(num);
    }
    return values;
}
export function parseCron(expression) {
    const fields = expression.trim().split(/\s+/);
    if (fields.length !== 5) {
        throw new Error(`Invalid cron expression: expected 5 fields, got ${fields.length}`);
    }
    return {
        minutes: parseField(fields[0], FIELD_RANGES[0][0], FIELD_RANGES[0][1]),
        hours: parseField(fields[1], FIELD_RANGES[1][0], FIELD_RANGES[1][1]),
        daysOfMonth: parseField(fields[2], FIELD_RANGES[2][0], FIELD_RANGES[2][1]),
        months: parseField(fields[3], FIELD_RANGES[3][0], FIELD_RANGES[3][1]),
        daysOfWeek: parseField(fields[4], FIELD_RANGES[4][0], FIELD_RANGES[4][1])
    };
}
export function cronMatches(parsed, date) {
    return (parsed.minutes.has(date.getUTCMinutes()) &&
        parsed.hours.has(date.getUTCHours()) &&
        parsed.daysOfMonth.has(date.getUTCDate()) &&
        parsed.months.has(date.getUTCMonth() + 1) &&
        parsed.daysOfWeek.has(date.getUTCDay()));
}
export function nextCronOccurrence(parsed, after) {
    // Start from the next minute
    const candidate = new Date(after.getTime());
    candidate.setUTCSeconds(0, 0);
    candidate.setUTCMinutes(candidate.getUTCMinutes() + 1);
    // Cap at 2 years to avoid infinite loops
    const limit = after.getTime() + 2 * 365 * 24 * 60 * 60 * 1000;
    while (candidate.getTime() <= limit) {
        if (cronMatches(parsed, candidate)) {
            return candidate;
        }
        candidate.setUTCMinutes(candidate.getUTCMinutes() + 1);
    }
    throw new Error('No matching cron occurrence found within 2 years');
}
