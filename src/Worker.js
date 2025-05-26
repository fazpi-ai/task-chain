import { EventEmitter } from 'events';
import { v4 } from 'uuid';
import os from 'os';

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
            maxDelay: params.backoff?.maxDelay || 1000 * 60 * 5 // 5 min
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
     * Calcula el tiempo de espera para reintentos usando backoff
     * @private
     */
    calculateBackoff(attempt) {
        if (this.backoff.type === 'exponential') {
            return Math.min(
                this.backoff.delay * Math.pow(2, attempt - 1),
                this.backoff.maxDelay
            );
        }
        return this.backoff.delay;
    }

    /**
     * Recorre los trabajos en estado "processing" sin lock y los reencola.
     */
    async recoverStalledJobs() {
        // Primero marcar todos los trabajos activos como potencialmente estancados
        const activeJobs = await this.connection.lrange(`${this.prefix}:active`, 0, -1);
        if (activeJobs.length) {
            await this.connection.sadd(`${this.prefix}:stalled`, ...activeJobs);
        }

        let cursor = '0';
        let recovered = 0;
        do {
            const [nextCursor, keys] = await this.connection.scan(cursor, 'MATCH', `${this.prefix}:job:*`, 'COUNT', 1000);
            if (keys.length) {
                const pipeline = this.connection.pipeline();
                keys.forEach((k) => {
                    pipeline.hget(k, 'status');
                    pipeline.hget(k, 'group');
                    pipeline.hget(k, 'attempts');
                });
                const results = await pipeline.exec();

                for (let idx = 0; idx < keys.length; idx++) {
                    const status = results[idx * 3][1];
                    const group = results[idx * 3 + 1][1] || 'default';
                    const attempts = parseInt(results[idx * 3 + 2][1] || '0');

                    if (status === 'processing') {
                        const jobId = keys[idx].split(':').pop();
                        const lockKey = `${this.prefix}:lock:${jobId}`;
                        const lockExists = await this.connection.exists(lockKey);
                        
                        if (!lockExists) {
                            const stalledCount = await this.connection.hincrby(keys[idx], 'stc', 1);
                            
                            if (stalledCount > this.maxStalledCount) {
                                // Move to failed
                                await this.connection.pipeline()
                                    .hset(keys[idx], 'status', 'failed', 'error', 'Job stalled too many times')
                                    .xadd(`${this.prefix}:events`, '*', 'event', 'failed', 'jobId', jobId, 'error', 'Job stalled too many times')
                                    .exec();
                            } else {
                                // Calculate retry delay
                                const delay = this.calculateBackoff(attempts + 1);
                                const retryAt = Date.now() + delay;

                                // requeue with delay
                                await this.connection.pipeline()
                                    .hset(keys[idx], 
                                        'status', 'delayed',
                                        'nextRetryAt', retryAt,
                                        'attempts', attempts + 1
                                    )
                                    .zadd(`${this.prefix}:delayed`, retryAt, jobId)
                                    .xadd(`${this.prefix}:events`, '*', 'event', 'delayed', 'jobId', jobId)
                                    .exec();
                            }

                            console.warn(`[Worker] Job ${jobId} recovered from stalled state (group: ${group})`);
                            recovered += 1;
                        }
                    }
                }
            }
            cursor = nextCursor;
        } while (cursor !== '0');

        // Limpiar set de trabajos estancados
        await this.connection.del(`${this.prefix}:stalled`);

        if (recovered > 0) {
            console.log(`[Worker] Stall recovery cycle completed. Recovered: ${recovered}`);
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
     * Procesa un job individual manteniendo los mapas de seguimiento
     * @private
     */
    async _handleJob(jobId, group) {
        const groupProcessingMap = this.processingByGroup.get(group);
        const jobKey = `${this.prefix}:job:${jobId}`;

        // Try to get the lock
        const token = await this.acquireLock(jobId);
        if (!token) {
            console.warn(`Could not get lock for job ${jobId}`);
            return;
        }

        // Register the job as being processed (global and by group)
        this.processingJobs.set(jobId, token);
        groupProcessingMap.set(jobId, Date.now());

        try {
            // Marcar como en proceso con token atomicamente
            const multi = this.connection.multi();
            multi.watch(jobKey);

            const jobData = await this.connection.hgetall(jobKey);
            if (!jobData) {
                await this.releaseLock(jobId, token);
                this.processingJobs.delete(jobId);
                groupProcessingMap.delete(jobId);
                return;
            }

            if (!jobData.data) jobData.data = '{}';

            let parsedData;
            try {
                parsedData = JSON.parse(jobData.data);
            } catch (parseError) {
                console.error(`Error parsing job data for ${jobId}:`, parseError);
                parsedData = {};
            }

            // Verificar y actualizar estado atomicamente
            multi
                .hset(jobKey,
                    'status', 'processing',
                    'token', token,
                    'processedOn', Date.now(),
                    'processedBy', this.workerId
                );

            const results = await multi.exec();
            if (!results) {
                console.warn(`Job ${jobId} was taken by another worker`);
                return;
            }

            this.emit('processing', { jobId, data: jobData });

            try {
                const result = await this.callback({ id: jobId, name: jobData.name || 'unknown', data: parsedData });

                // Confirmar completado solo si aún tenemos el token
                const completePipeline = this.connection.multi();
                completePipeline.watch(jobKey);

                const currentToken = await this.connection.hget(jobKey, 'token');
                if (currentToken !== token) {
                    throw new Error('Job token mismatch');
                }

                completePipeline
                    .hset(jobKey,
                        'status', 'completed',
                        'finishedOn', Date.now(),
                        'result', JSON.stringify(result)
                    )
                    .hdel(jobKey, 'token')
                    .exec();

                this.emit('completed', { jobId });

            } catch (error) {
                // Solo marcar como fallido si aún tenemos el token
                const currentToken = await this.connection.hget(jobKey, 'token');
                if (currentToken === token) {
                    const attempts = await this.connection.hincrby(jobKey, 'attempts', 1);
                    const maxAttempts = parseInt(jobData.maxAttempts || '3');

                    if (attempts < maxAttempts) {
                        // Calcular delay para reintento
                        const delay = this.calculateBackoff(attempts);
                        const retryAt = Date.now() + delay;

                        await this.connection.pipeline()
                            .hset(jobKey,
                                'status', 'delayed',
                                'nextRetryAt', retryAt,
                                'error', error.message
                            )
                            .zadd(`${this.prefix}:delayed`, retryAt, jobId)
                            .hdel(jobKey, 'token')
                            .xadd(`${this.prefix}:events`, '*', 'event', 'delayed', 'jobId', jobId)
                            .exec();
                    } else {
                        await this.connection.pipeline()
                            .hset(jobKey,
                                'status', 'failed',
                                'error', error.message,
                                'finishedOn', Date.now()
                            )
                            .hdel(jobKey, 'token')
                            .xadd(`${this.prefix}:events`, '*', 'event', 'failed', 'jobId', jobId, 'error', error.message)
                            .exec();
                    }
                }

                this.emit('failed', { jobId, error });
            }

        } catch (error) {
            console.error(`Error processing job ${jobId}:`, error);
            this.emit('error', { jobId, error });
        } finally {
            await this.releaseLock(jobId, token);
            this.processingJobs.delete(jobId);
            groupProcessingMap.delete(jobId);
        }
    }
}

export default Worker;