import { beforeEach, describe, expect, test } from 'bun:test';
import { SlidingWindowRateLimiter } from './rate-limiter';

describe('SlidingWindowRateLimiter', () => {
  let limiter: SlidingWindowRateLimiter;

  beforeEach(() => {
    limiter = new SlidingWindowRateLimiter(3, 1000);
  });

  describe('given a fresh limiter, when canProceed is called', () => {
    test('then it allows up to maxCount executions', () => {
      expect(limiter.canProceed()).toBe(true);
      limiter.record();
      expect(limiter.canProceed()).toBe(true);
      limiter.record();
      expect(limiter.canProceed()).toBe(true);
      limiter.record();
      expect(limiter.canProceed()).toBe(false);
    });
  });

  describe('given a saturated limiter, when the window expires', () => {
    test('then it allows executions again', async () => {
      const shortLimiter = new SlidingWindowRateLimiter(2, 50);
      shortLimiter.record();
      shortLimiter.record();
      expect(shortLimiter.canProceed()).toBe(false);

      await new Promise(resolve => setTimeout(resolve, 60));

      expect(shortLimiter.canProceed()).toBe(true);
    });
  });

  describe('given a saturated limiter, when reset is called', () => {
    test('then it clears all history', () => {
      limiter.record();
      limiter.record();
      limiter.record();
      expect(limiter.canProceed()).toBe(false);

      limiter.reset();
      expect(limiter.canProceed()).toBe(true);
    });
  });

  describe('given a sliding window, when old entries expire', () => {
    test('then only expired entries are pruned', async () => {
      const shortLimiter = new SlidingWindowRateLimiter(2, 50);
      shortLimiter.record();

      await new Promise(resolve => setTimeout(resolve, 30));
      shortLimiter.record();

      await new Promise(resolve => setTimeout(resolve, 30));
      // First entry expired, second still valid
      expect(shortLimiter.canProceed()).toBe(true);
    });
  });
});
