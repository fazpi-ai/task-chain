import { EventEmitter } from 'events';
import { v4 } from 'uuid';
import os from 'os';

// Errores personalizados
export class UnrecoverableError extends Error {
    constructor(message) {
        super(message);
        this.name = 'UnrecoverableError';
    }
}

export class RateLimitError extends Error {
    constructor(message = 'Rate limit reached') {
        super(message);
        this.name = 'RateLimitError';
    }
}

class Worker extends EventEmitter {
    constructor(appName, queueName, callback, params) {
        super();

        this.prefix = `${appName}:${queueName}`;

        this.appName = appName;
        this.queueName = queueName;
        
        if (!params.connection) {
            throw new Error('A valid Redis connection is required');
        }
        this.connection = params.connection;

        // Worker unique identifier and token counter
        this.workerId = v4();
        this.tokenPostfix = 0;

        // Callback to process the job
        this.callback = callback;

        // Maps for tracking jobs
        this.processingJobs = new Map(); // Global map of jobs being processed: jobId -> token
        this.processingByGroup = new Map(); // Map of jobs being processed by group

        // ==== Soporte de grupos ====
        this.groupConcurrency = 0; // se fijará tras leer this.concurrency
        this.groups = params.groups || [];
        this.groupsSetKey = `${this.prefix}:groups:set`;
        this.currentGroupIdx = 0; // index for round-robin

        // Helper to get list of groups from Redis
        this.refreshGroups = async () => {
            try {
                const groupsFromRedis = await this.connection.smembers(this.groupsSetKey);
                if (Array.isArray(groupsFromRedis) && groupsFromRedis.length > 0) {
                    this.groups = groupsFromRedis.sort(); // consistent order
                }
            } catch (err) {
                console.error('Error getting groups:', err);
            }
        };

        // Initialize map by group (if there are predefined groups)
        for (const g of this.groups) {
            this.processingByGroup.set(g, new Map());
        }

        // Execution parameters (global)
        this.concurrency = params.concurrency || 1;
        this.groupConcurrency = this.concurrency; // usar el mismo límite por grupo
        this.removeOnComplete = params.removeOnComplete || false;
        this.removeOnFail = params.removeOnFail || 5000;
        this.batchSize = params.batchSize || 1;
        this.pollInterval = params.pollInterval || 1000;
        this.stallInterval = typeof params.stallInterval === 'number' && params.stallInterval >= 0
            ? params.stallInterval
            : 5000;
        this.lockDuration = params.lockDuration || 30000; // Job lock duration (global)
        this.maxStalledCount = params.maxStalledCount || 3; // Max number of times a job can stall

        // Retry configuration
        this.backoff = {
            type: params.backoff?.type || 'exponential',
            delay: params.backoff?.delay || 1000,
            maxDelay: params.backoff?.maxDelay || 1000 * 60 * 5, // 5 min
            strategy: params.backoff?.strategy, // función personalizada de backoff
            // Porcentaje de jitter (0-1). true == 0.25. 0 o undefined = sin jitter
            jitter: params.backoff?.jitter
        };

        // Worker health monitoring
        this.heartbeatInterval = params.heartbeatInterval || 5000;
        this.heartbeatKey = `${this.prefix}:workers:${this.workerId}`;

        // Execution control (global)
        this.isRunning = false;

        // Timers
        this._lockInterval = null;
        this._stallIntervalId = null;
        this._lockExtenderTimer = null;
        this._heartbeatTimer = null;

        // util sleep
        this._sleep = (ms) => new Promise((res) => setTimeout(res, ms));

        /**
         * Elimina o expira un job según configuración.
         * @param {string} jobKey clave completa del job
         * @param {'completed'|'failed'} status
         * @private
         */
        this._handleJobRemoval = async (jobKey, status) => {
            try {
                let cfg;
                if (status === 'completed') cfg = this.removeOnComplete;
                else if (status === 'failed') cfg = this.removeOnFail;

                if (!cfg) return; // falsy ⇒ no eliminar

                if (cfg === true || cfg === 0) {
                    // eliminación inmediata
                    await this.connection.del(jobKey);
                } else if (typeof cfg === 'number' && cfg > 0) {
                    // expiración en TTL ms
                    await this.connection.pexpire(jobKey, cfg);
                }
            } catch (err) {
                console.error('Error scheduling job removal:', err);
            }
        };

        // Configuración de trabajos estancados
        this.stalledCheckInterval = params.stalledCheckInterval || 30000;
        this.stalledTimeout = params.stalledTimeout || this.lockDuration * 2;
    }

    /**
     * Get a lock for a job (global)
     * @private
     */
    async acquireLock(jobId) {
        const token = `${this.workerId}:${this.tokenPostfix++}`;
        const lockKey = `${this.prefix}:lock:${jobId}`;
        const result = await this.connection.set(
            lockKey,
            token,
            'PX',
            this.lockDuration,
            'NX'
        );
        return result === 'OK' ? token : null;
    }

    /**
     * Renew the lock for a job
     * @private
     */
    async renewLock(jobId, token) {
        const lockKey = `${this.prefix}:lock:${jobId}`;
        const currentToken = await this.connection.get(lockKey);
        
        // Only extend if we still own the lock
        if (currentToken === token) {
            return await this.connection.pexpire(lockKey, this.lockDuration);
        }
        return false;
    }

    /**
     * Release the lock for a job
     * @private
     */
    async releaseLock(jobId, token) {
        const lockKey = `${this.prefix}:lock:${jobId}`;
        const currentToken = await this.connection.get(lockKey);
        
        // Only delete if we own the lock
        if (currentToken === token) {
            await this.connection.del(lockKey);
        }
    }

    async start() {
        this.isRunning = true;

        if (this.concurrency < 1 || !Number.isFinite(this.concurrency)) {
            throw new Error('The concurrency must be a finite number greater than 0');
        }

        // Register worker and start heartbeat
        await this.connection.pipeline()
            .hset(this.heartbeatKey,
                'started', Date.now(),
                'host', os.hostname(),
                'pid', process.pid,
                'concurrency', this.concurrency
            )
            .pexpire(this.heartbeatKey, this.heartbeatInterval * 2)
            .exec();

        this._heartbeatTimer = setInterval(() => {
            this.connection.pipeline()
                .hset(this.heartbeatKey, 'lastBeat', Date.now())
                .pexpire(this.heartbeatKey, this.heartbeatInterval * 2)
                .exec()
                .catch(console.error);
        }, this.heartbeatInterval);

        // Start the lock renewal interval
        this.startLockExtender();

        // Intervalo para recuperar trabajos estancados
        this._stallIntervalId = setInterval(() => {
            this.recoverStalledJobs().catch(console.error);
        }, this.stallInterval);

        try {
            while (this.isRunning) {
                try {
                    // ================= ROUND-ROBIN BY GROUPS =================
                    // Ensure we have groups; refresh periodically
                    if (this.groups.length === 0 || this.currentGroupIdx === 0) {
                        await this.refreshGroups();
                    }

                    if (this.groups.length === 0) {
                        await new Promise(resolve => setTimeout(resolve, this.pollInterval));
                        continue; // No groups yet
                    }

                    const group = this.groups[this.currentGroupIdx];
                    this.currentGroupIdx = (this.currentGroupIdx + 1) % this.groups.length;

                    // Map for the current group
                    if (!this.processingByGroup.has(group)) {
                        this.processingByGroup.set(group, new Map());
                    }
                    const groupProcessingMap = this.processingByGroup.get(group);

                    // Respect concurrency
                    const freeGlobal = this.concurrency - this.processingJobs.size;
                    const freeGroup = this.groupConcurrency - groupProcessingMap.size;
                    if (freeGlobal <= 0 || freeGroup <= 0) {
                        await new Promise(resolve => setTimeout(resolve, 0));
                        continue;
                    }

                    const currentBatchSize = Math.min(this.batchSize, freeGlobal, freeGroup);

                    // Get jobs from the group atomically
                    const jobs = await this.connection.pipeline()
                        .rpop(`${this.prefix}:groups:${group}`, currentBatchSize)
                        .exec();

                    const jobIds = jobs
                        .filter(([err, result]) => !err && result)
                        .map(([_, result]) => result)
                        .flat();

                    if (jobIds.length === 0) {
                        await new Promise(resolve => setTimeout(resolve, this.pollInterval));
                        continue;
                    }

                    // Launch jobs asynchronously without blocking the main loop
                    for (const jobId of jobIds) {
                        this._handleJob(jobId, group).catch((err) => {
                            console.error('Unhandled error in _handleJob:', err);
                        });
                    }

                } catch (error) {
                    console.error('Error in batch processing:', error);
                    await new Promise(resolve => setTimeout(resolve, this.pollInterval));
                }
            }
        } finally {
            clearInterval(this._lockInterval);
            clearInterval(this._stallIntervalId);
            clearInterval(this._lockExtenderTimer);
            clearInterval(this._heartbeatTimer);
            await this.connection.del(this.heartbeatKey);
        }
    }

    /**
     * Detiene el worker de forma graceful.
     * Espera a que los trabajos en curso finalicen (hasta timeout)
     * @param {number} timeoutMs tiempo máximo a esperar en ms (default 30000)
     */
    async stop(timeoutMs = 30000) {
        this.isRunning = false;

        const start = Date.now();
        while (this.processingJobs.size > 0 && (Date.now() - start) < timeoutMs) {
            await this._sleep(100);
        }

        if (this.processingJobs.size > 0) {
            console.warn('Worker detenido con trabajos aún en proceso. Considera aumentar el timeout.');
        }

        // limpiar intervalos y estado del worker
        if (this._lockInterval) clearInterval(this._lockInterval);
        if (this._stallIntervalId) clearInterval(this._stallIntervalId);
        if (this._lockExtenderTimer) clearInterval(this._lockExtenderTimer);
        if (this._heartbeatTimer) clearInterval(this._heartbeatTimer);
        await this.connection.del(this.heartbeatKey);
    }

    /**
     * Calcula el tiempo de espera para el próximo reintento
     * @private
     */
    calculateBackoff(attempt, jobData = null, error = null) {
        const { type, delay, maxDelay, strategy, jitter } = this.backoff;
        
        // Si hay una estrategia personalizada, usarla
        if (typeof strategy === 'function') {
            return strategy(attempt, jobData, error);
        }

        let nextDelay;
        
        switch (type) {
            case 'exponential':
                nextDelay = Math.min(delay * Math.pow(2, attempt - 1), maxDelay);
                break;
            case 'linear':
                nextDelay = Math.min(delay * attempt, maxDelay);
                break;
            default: // fixed
                nextDelay = delay;
        }

        // Aplicar jitter si está configurado
        if (jitter) {
            const jitterAmount = typeof jitter === 'number' ? jitter : 0.25;
            const randomFactor = 1 - jitterAmount + (Math.random() * jitterAmount * 2);
            nextDelay = Math.floor(nextDelay * randomFactor);
        }

        return nextDelay;
    }

    /**
     * Mejora del manejo de trabajos estancados
     */
    async recoverStalledJobs() {
        const now = Date.now();
        const stalledSet = `${this.prefix}:stalled`;
        const activeSet = `${this.prefix}:active`;

        try {
            // 1. Obtener trabajos activos
            const activeJobs = await this.connection.zrangebyscore(
                activeSet,
                0,
                now - this.stalledTimeout
            );

            if (!activeJobs.length) return;

            // 2. Verificar cada trabajo
            for (const jobId of activeJobs) {
                const jobKey = `${this.prefix}:job:${jobId}`;
                const lockKey = `${this.prefix}:lock:${jobId}`;

                const [job, hasLock] = await this.connection.pipeline()
                    .hgetall(jobKey)
                    .exists(lockKey)
                    .exec();

                if (!job[1] || !job[1].status) continue;
                if (hasLock[1]) continue; // El trabajo aún tiene un lock válido

                const stalledCount = parseInt(job[1].stalledCount || '0') + 1;
                const opts = JSON.parse(job[1].opts || '{}');
                const maxStalls = opts.maxStalledCount || this.maxStalledCount;

                if (stalledCount >= maxStalls) {
                    // El trabajo ha excedido el número máximo de stalls
                    await this.connection.pipeline()
                        .hset(jobKey,
                            'status', 'failed',
                            'failedReason', `Stalled ${stalledCount} times`,
                            'finishedOn', now
                        )
                        .zadd(`${this.prefix}:failed`, now, jobId)
                        .zrem(activeSet, jobId)
                        .exec();

                    this.emit('failed', {
                        jobId,
                        error: new Error(`Job stalled ${stalledCount} times`),
                        stalled: true
                    });
                } else {
                    // Reintentar el trabajo
                    const group = job[1].group || 'default';
                    const delay = this.calculateBackoff(stalledCount, job[1]);
                    const nextProcessAt = now + delay;

                    await this.connection.pipeline()
                        .hset(jobKey,
                            'status', 'delayed',
                            'stalledCount', stalledCount,
                            'nextProcessAt', nextProcessAt
                        )
                        .zadd(`${this.prefix}:delayed`, nextProcessAt, jobId)
                        .zrem(activeSet, jobId)
                        .exec();

                    this.emit('stalled', { jobId, stalledCount, nextProcessAt });
                }
            }
        } catch (error) {
            console.error('Error recovering stalled jobs:', error);
        }
    }

    /**
     * Inicia el timer para extender los locks de trabajos en proceso
     * @private
     */
    startLockExtender() {
        this._lockExtenderTimer = setInterval(() => {
            for (const [jobId, token] of this.processingJobs) {
                this.renewLock(jobId, token).catch(console.error);
            }
        }, Math.floor(this.lockDuration / 2));
    }

    /**
     * Maneja el procesamiento de un trabajo
     * @private
     */
    async _handleJob(jobId, group) {
        const jobKey = `${this.prefix}:job:${jobId}`;
        const groupProcessingMap = this.processingByGroup.get(group);

        // Obtener lock
        const token = await this.acquireLock(jobId);
        if (!token) return;

        try {
            // Obtener datos del trabajo
            const jobData = await this.connection.hgetall(jobKey);
            if (!jobData) {
                await this.releaseLock(jobId, token);
                return;
            }

            // Parsear datos
            try {
                jobData.data = JSON.parse(jobData.data);
                jobData.opts = JSON.parse(jobData.opts);
            } catch (e) {
                console.error(`Error parsing job data for ${jobId}:`, e);
                throw new UnrecoverableError('Invalid job data format');
            }

            // Registrar el trabajo en proceso
            this.processingJobs.set(jobId, token);
            groupProcessingMap.set(jobId, token);

            // Actualizar metadatos del trabajo
            await this.connection.pipeline()
                .hset(jobKey,
                    'status', 'active',
                    'processedOn', Date.now(),
                    'workerId', this.workerId
                )
                .hincrby(jobKey, 'attemptsStarted', 1)
                .exec();

            try {
                // Procesar el trabajo
                const result = await this.callback(jobData, token);

                // Marcar como completado
                await this.connection.pipeline()
                    .hset(jobKey,
                        'status', 'completed',
                        'finishedOn', Date.now(),
                        'returnvalue', JSON.stringify(result)
                    )
                    .zadd(`${this.prefix}:completed`, Date.now(), jobId)
                    .hdel(jobKey, 'token')
                    .exec();

                this.emit('completed', { jobId, result });

                // Manejar eliminación si está configurado
                await this._handleJobRemoval(jobKey, 'completed');

            } catch (error) {
                // Determinar si el error es recuperable
                const isUnrecoverable = error instanceof UnrecoverableError;
                const isRateLimit = error instanceof RateLimitError;
                
                if (!isRateLimit) {
                    await this.connection.hincrby(jobKey, 'attemptsMade', 1);
                }

                const attempts = parseInt(await this.connection.hget(jobKey, 'attemptsMade')) || 0;
                const maxAttempts = jobData.opts.attempts || 1;

                if (isUnrecoverable || attempts >= maxAttempts) {
                    // Marcar como fallido permanentemente
                    await this.connection.pipeline()
                        .hset(jobKey,
                            'status', 'failed',
                            'failedReason', error.message,
                            'finishedOn', Date.now()
                        )
                        .zadd(`${this.prefix}:failed`, Date.now(), jobId)
                        .hdel(jobKey, 'token')
                        .exec();

                    this.emit('failed', { jobId, error });
                    
                    if (attempts >= maxAttempts) {
                        this.emit('retries-exhausted', { jobId, attemptsMade: attempts });
                    }

                    // Manejar eliminación si está configurado
                    await this._handleJobRemoval(jobKey, 'failed');
                } else {
                    // Calcular delay para reintento
                    const delay = this.calculateBackoff(attempts + 1, jobData, error);
                    const nextProcessAt = Date.now() + delay;

                    // Mover a delayed para reintento
                    await this.connection.pipeline()
                        .hset(jobKey,
                            'status', 'delayed',
                            'failedReason', error.message,
                            'nextProcessAt', nextProcessAt
                        )
                        .zadd(`${this.prefix}:delayed`, nextProcessAt, jobId)
                        .hdel(jobKey, 'token')
                        .exec();

                    this.emit('failed', { jobId, error, willRetry: true });
                }
            }
        } finally {
            // Limpieza
            this.processingJobs.delete(jobId);
            groupProcessingMap.delete(jobId);
            await this.releaseLock(jobId, token);
        }
    }
}

export default Worker;