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
        const groupConfig = params.group || {};
        this.groupConcurrency = Number(groupConfig.concurrency) || 1; // max. per group

        // Map of jobs being processed by group
        this.processingByGroup = new Map();

        // List of discovered groups; if the user passes groups in params we use it
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
        this.removeOnComplete = params.removeOnComplete || false;
        this.removeOnFail = params.removeOnFail || 5000;
        this.batchSize = params.batchSize || 1;
        this.pollInterval = params.pollInterval || 1000;
        this.stallInterval = params.stallInterval || 30000; // Interval to check stalled jobs (global)
        this.lockDuration = params.lockDuration || 30000; // Job lock duration (global)

        // Execution control (global)
        this.isRunning = false;
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
        const lockInterval = setInterval(() => {
            for (const [jobId] of this.processingJobs) {
                this.renewLock(jobId).catch(console.error);
            }
        }, Math.floor(this.lockDuration / 2));

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

                    // Process jobs in parallel
                    await Promise.all(jobIds.map(async (jobId) => {
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

                            // Ensure we have valid data
                            if (!jobData.data) {
                                jobData.data = '{}';
                            }

                            // Validate that the JSON is valid before processing it
                            let parsedData;
                            try {
                                parsedData = JSON.parse(jobData.data);
                            } catch (parseError) {
                                console.error(`Error parsing job data for ${jobId}:`, parseError);
                                jobData.data = '{}';
                                parsedData = {};
                            }

                            // Remove any previous errors and update status
                            await this.connection.pipeline()
                                .hdel(jobKey, 'error')
                                .hset(jobKey, 'status', 'processing')
                                .exec();

                            // Emit the start event
                            this.emit('processing', { jobId, data: jobData });

                            try {
                                // Process the job
                                await this.callback({
                                    id: jobId,
                                    name: jobData.name || 'unknown',
                                    data: parsedData
                                });

                                // Mark as completed
                                await this.connection.pipeline()
                                    .hset(jobKey, 'status', 'completed')
                                    .xadd(
                                        `${this.prefix}:events`,
                                        '*',
                                        'event', 'completed',
                                        'jobId', jobId
                                    )
                                    .exec();

                                this.emit('completed', { jobId });

                            } catch (error) {
                                // Mark as failed
                                await this.connection.pipeline()
                                    .hset(jobKey,
                                        'status', 'failed',
                                        'error', error.message
                                    )
                                    .xadd(
                                        `${this.prefix}:events`,
                                        '*',
                                        'event', 'failed',
                                        'jobId', jobId,
                                        'error', error.message
                                    )
                                    .exec();

                                this.emit('failed', { jobId, error });
                            }

                        } catch (error) {
                            console.error(`Error processing job ${jobId}:`, error);
                            this.emit('error', { jobId, error });
                        } finally {
                            // Clean up
                            await this.releaseLock(jobId);
                            this.processingJobs.delete(jobId);
                            groupProcessingMap.delete(jobId);
                        }
                    }));

                } catch (error) {
                    console.error('Error in batch processing:', error);
                    await new Promise(resolve => setTimeout(resolve, this.pollInterval));
                }
            }
        } finally {
            clearInterval(lockInterval);
        }
    }

    stop() {
        this.isRunning = false;
    }
}

export default Worker;