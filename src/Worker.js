import { EventEmitter } from 'events';

class Worker extends EventEmitter {
    constructor(appName, queueName, callback, params) {
        super();

        this.prefix = `${appName}:${queueName}`;

        this.appName = appName;
        this.queueName = queueName;
        
        // Ensure we have a valid Redis connection
        if (!params.connection) {
            throw new Error('A valid Redis connection is required');
        }
        this.connection = params.connection;

        // Callback to process the job
        this.callback = callback;

        this.processingJobs = new Map(); // Global map of jobs being processed

        // ==== Soporte de grupos ====
        this.groupConcurrency = 0; // se fijará tras leer this.concurrency

        // Map of jobs being processed by group
        this.processingByGroup = new Map();

        // List of discovered groups; if the user passes groups in params.groups lo usamos
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
        // this.stallInterval se define más abajo junto con control interno
        this.lockDuration = params.lockDuration || 30000; // Job lock duration (global)

        // Execution control (global)
        this.isRunning = false;

        // Intervalo para detectar trabajos estancados (que quedaron en "processing" sin lock)
        // this.stallInterval se define más abajo junto con control interno

        this._lockInterval = null; // se establecerá en start()
        this._stallIntervalId = null; // intervalo de verificación de estancados

        // util sleep
        this._sleep = (ms) => new Promise((res) => setTimeout(res, ms));
    }

    /**
   * Get a lock for a job (global)
   * @private
   */
    async acquireLock(jobId) {
        const lockKey = `${this.prefix}:lock:${jobId}`;
        const result = await this.connection.set(
            lockKey,
            '1',
            'PX',
            this.lockDuration,
            'NX'
        );
        return result === 'OK';
    }

    /**
     * Renew the lock for a job
     * @private
     */
    async renewLock(jobId) {
        const lockKey = `${this.prefix}:lock:${jobId}`;
        return await this.connection.pexpire(lockKey, this.lockDuration);
    }

    /**
     * Release the lock for a job
     * @private
     */
    async releaseLock(jobId) {
        const lockKey = `${this.prefix}:lock:${jobId}`;
        await this.connection.del(lockKey);
    }

    async start() {
        this.isRunning = true;

        if (this.concurrency < 1 || !Number.isFinite(this.concurrency)) {
            throw new Error('The concurrency must be a finite number greater than 0');
        }

        // Start the lock renewal interval
        this._lockInterval = setInterval(() => {
            for (const [jobId] of this.processingJobs) {
                this.renewLock(jobId).catch(console.error);
            }
        }, Math.floor(this.lockDuration / 2));

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

                    // Get jobs from the group
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

        // limpiar intervalos por si start no fue alcanzado completamente
        if (this._lockInterval) clearInterval(this._lockInterval);
        if (this._stallIntervalId) clearInterval(this._stallIntervalId);
    }

    /**
     * Recorre los trabajos en estado "processing" sin lock y los reencola.
     */
    async recoverStalledJobs() {
        let cursor = '0';
        let recovered = 0;
        do {
            const [nextCursor, keys] = await this.connection.scan(cursor, 'MATCH', `${this.prefix}:job:*`, 'COUNT', 1000);
            if (keys.length) {
                const pipeline = this.connection.pipeline();
                keys.forEach((k) => {
                    pipeline.hget(k, 'status');
                    pipeline.hget(k, 'group');
                });
                const results = await pipeline.exec();

                for (let idx = 0; idx < keys.length; idx++) {
                    const status = results[idx * 2][1];
                    const group = results[idx * 2 + 1][1] || 'default';
                    if (status === 'processing') {
                        const jobId = keys[idx].split(':').pop();
                        const lockKey = `${this.prefix}:lock:${jobId}`;
                        const lockExists = await this.connection.exists(lockKey);
                        if (!lockExists) {
                            // requeue
                            await this.connection.pipeline()
                                .hset(keys[idx], 'status', 'waiting')
                                .lpush(`${this.prefix}:groups:${group}`, jobId)
                                .xadd(`${this.prefix}:events`, '*', 'event', 'stalled', 'jobId', jobId)
                                .exec();

                            console.warn(`[Worker] Job ${jobId} recovered from stalled state (group: ${group})`);
                            recovered += 1;
                        }
                    }
                }
            }
            cursor = nextCursor;
        } while (cursor !== '0');

        if (recovered > 0) {
            console.log(`[Worker] Stall recovery cycle completed. Recovered: ${recovered}`);
        }
    }

    /**
     * Procesa un job individual manteniendo los mapas de seguimiento
     * @private
     */
    async _handleJob(jobId, group) {
        const groupProcessingMap = this.processingByGroup.get(group);
        const jobKey = `${this.prefix}:job:${jobId}`;

        // Try to get the lock
        if (!await this.acquireLock(jobId)) {
            console.warn(`Could not get lock for job ${jobId}`);
            return;
        }

        // Register the job as being processed (global and by group)
        this.processingJobs.set(jobId, Date.now());
        groupProcessingMap.set(jobId, Date.now());

        try {
            const jobData = await this.connection.hgetall(jobKey);
            if (!jobData) {
                await this.releaseLock(jobId);
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

            await this.connection.pipeline()
                .hdel(jobKey, 'error')
                .hset(jobKey, 'status', 'processing')
                .exec();

            this.emit('processing', { jobId, data: jobData });

            try {
                await this.callback({ id: jobId, name: jobData.name || 'unknown', data: parsedData });

                await this.connection.pipeline()
                    .hset(jobKey, 'status', 'completed')
                    .xadd(`${this.prefix}:events`, '*', 'event', 'completed', 'jobId', jobId)
                    .exec();

                this.emit('completed', { jobId });

            } catch (error) {
                await this.connection.pipeline()
                    .hset(jobKey, 'status', 'failed', 'error', error.message)
                    .xadd(`${this.prefix}:events`, '*', 'event', 'failed', 'jobId', jobId, 'error', error.message)
                    .exec();

                this.emit('failed', { jobId, error });
            }

        } catch (error) {
            console.error(`Error processing job ${jobId}:`, error);
            this.emit('error', { jobId, error });
        } finally {
            await this.releaseLock(jobId);
            this.processingJobs.delete(jobId);
            groupProcessingMap.delete(jobId);
        }
    }
}

export default Worker;