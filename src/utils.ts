// packages/jobs/src/utils.ts
// Shared utilities for the jobs package

export function nowISO(): string {
  return `${new Date().toISOString().replace('Z', '').slice(0, 23)}Z`;
}
