import { config } from '../config';

export interface CircuitBreakerState {
  sinkName: string;
  isAvailable: boolean;
  consecutiveFailures: number;
  lastFailureAt: string | null;
  lastRecoveredAt: string | null;
  currentBackoffMs: number;
}

export class CircuitBreaker {
  private sinkName: string;
  private isAvailable: boolean = true;
  private consecutiveFailures: number = 0;
  private lastFailureAt: Date | null = null;
  private lastRecoveredAt: Date | null = null;
  private currentBackoffMs: number;
  private maxBackoffMs: number;
  private initialBackoffMs: number;

  constructor(sinkName: string) {
    this.sinkName = sinkName;
    this.initialBackoffMs = config.pipeline.initialBackoffMs;
    this.maxBackoffMs = config.pipeline.maxBackoffMs;
    this.currentBackoffMs = this.initialBackoffMs;
  }

  /**
   * Executes an operation through the circuit breaker.
   * On failure, blocks with exponential backoff and jitter until health is restored.
   */
  public async executeWithResilience<T>(
    operation: () => Promise<T>,
    healthProbe: () => Promise<boolean>
  ): Promise<T> {
    while (true) {
      try {
        const result = await operation();
        this.recordSuccess();
        return result;
      } catch (err: any) {
        this.recordFailure(err);

        // Calculate backoff with jitter: delay = min(base * 2^failures + jitter, max)
        const jitter = Math.floor(Math.random() * 500);
        const delay = Math.min(
          this.initialBackoffMs * Math.pow(2, Math.min(this.consecutiveFailures - 1, 6)) + jitter,
          this.maxBackoffMs
        );
        this.currentBackoffMs = delay;

        console.warn(
          `[CircuitBreaker:${this.sinkName}] Sink failure detected (${err.message}). Entering backoff for ${delay}ms (Failures: ${this.consecutiveFailures})`
        );

        // Sleep to avoid CPU busy-looping (Invariant G3)
        await new Promise((resolve) => setTimeout(resolve, delay));

        // Probe health before next retry attempt
        let healthy = false;
        try {
          healthy = await healthProbe();
        } catch {
          healthy = false;
        }

        if (!healthy) {
          console.warn(`[CircuitBreaker:${this.sinkName}] Health probe still failing, continuing backoff...`);
        }
      }
    }
  }

  private recordSuccess(): void {
    if (!this.isAvailable) {
      this.lastRecoveredAt = new Date();
      console.log(`[CircuitBreaker:${this.sinkName}] Sink successfully recovered after ${this.consecutiveFailures} failures`);
    }
    this.isAvailable = true;
    this.consecutiveFailures = 0;
    this.currentBackoffMs = this.initialBackoffMs;
  }

  private recordFailure(err: any): void {
    this.isAvailable = false;
    this.consecutiveFailures++;
    this.lastFailureAt = new Date();
  }

  public getState(): CircuitBreakerState {
    return {
      sinkName: this.sinkName,
      isAvailable: this.isAvailable,
      consecutiveFailures: this.consecutiveFailures,
      lastFailureAt: this.lastFailureAt ? this.lastFailureAt.toISOString() : null,
      lastRecoveredAt: this.lastRecoveredAt ? this.lastRecoveredAt.toISOString() : null,
      currentBackoffMs: this.currentBackoffMs,
    };
  }
}
