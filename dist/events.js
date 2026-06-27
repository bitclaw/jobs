export class JobQueueEmitter {
    listeners = new Map();
    on(event, handler) {
        if (!this.listeners.has(event)) {
            this.listeners.set(event, new Set());
        }
        this.listeners.get(event).add(handler);
        return () => this.off(event, handler);
    }
    off(event, handler) {
        this.listeners.get(event)?.delete(handler);
    }
    once(event, handler) {
        const wrapper = ((...args) => {
            this.off(event, wrapper);
            handler(...args);
        });
        this.on(event, wrapper);
    }
    emit(event, ...args) {
        const handlers = this.listeners.get(event);
        if (!handlers)
            return;
        for (const handler of handlers) {
            try {
                handler(...args);
            }
            catch {
                // silently swallow listener errors
            }
        }
    }
}
