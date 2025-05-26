import { readFileSync } from 'fs';
import { fileURLToPath } from 'url';
import { dirname, join } from 'path';

// Getting __dirname in ES modules
const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

class Queue {

    constructor(appName, queueName, params) {
        this.prefix = `${appName}:${queueName}`;
        this.appName = appName;
        this.queueName = queueName;
        
        if (!params.connection) {
            throw new Error('A valid Redis connection is required');
        }
        this.connection = params.connection;

        // Load Lua script
        this.addJobScript = readFileSync(
            join(__dirname, 'commands', 'addJob-8.lua'),
            'utf8'
        );
    }

    /**
     * Cleans the entire Redis database
     * @param {Object} connection - Redis connection
     * @returns {Promise<void>}
     */
    static async flushAll(connection) {
        if (!connection) {
            throw new Error('A valid Redis connection is required');
        }
        await connection.flushall();
        console.log('Redis database cleanup completely');
    }

    /**
     * Añade un trabajo a la cola
     * @param {string} jobName - Nombre del trabajo
     * @param {Object} data - Datos del trabajo
     * @param {Object} [opts={}] - Opciones del trabajo
     * @returns {Promise<string>} ID del trabajo
     */
    async add(jobName, data, opts = {}) {
        const timestamp = Date.now();

        // Determine group
        let group = 'default';
        if (data && typeof data === 'object' && data.group && typeof data.group === 'object' && data.group.id) {
            group = data.group.id;
        }

        // Validate options
        const jobOptions = {
            attempts: opts.attempts || 3,
            backoff: opts.backoff || {
                type: 'exponential',
                delay: 1000
            },
            removeOnComplete: opts.removeOnComplete || false,
            removeOnFail: opts.removeOnFail || false,
            ...opts
        };

        // Keys that will be used by the Lua script
        const keys = [
            `${this.prefix}:wait`,             // Waiting queue (legacy / compat)
            `${this.prefix}:meta`,             // Metadata
            `${this.prefix}:counter`,          // ID counter
            `${this.prefix}:events`,           // Events stream
            `${this.prefix}:groups:${group}`,  // Group specific queue
            `${this.prefix}:groups:set`,       // Groups set
            `${this.prefix}:delayed`,          // Delayed jobs sorted set
        ];

        // Arguments for the script
        const args = [
            jobName,                           // Job name
            JSON.stringify(data),              // Job data
            JSON.stringify(jobOptions),        // Job options
            timestamp.toString(),              // Timestamp
            this.prefix,                       // Prefix for other keys
            group                              // Group name
        ];

        try {
            // Load and execute the script atomically
            const jobId = await this.connection.eval(
                this.addJobScript,
                keys.length,
                ...keys,
                ...args
            );

            return jobId.toString();
        } catch (error) {
            console.error('Error adding job:', error);
            throw error;
        }
    }

    /**
     * Obtiene información de un trabajo
     * @param {string} jobId - ID del trabajo
     * @returns {Promise<Object>} Datos del trabajo
     */
    async getJob(jobId) {
        const jobKey = `${this.prefix}:job:${jobId}`;
        const jobData = await this.connection.hgetall(jobKey);
        
        if (!jobData || Object.keys(jobData).length === 0) {
            return null;
        }

        // Parse JSON fields
        if (jobData.data) {
            try {
                jobData.data = JSON.parse(jobData.data);
            } catch (e) {
                console.warn(`Could not parse job data for ${jobId}`);
            }
        }

        if (jobData.opts) {
            try {
                jobData.opts = JSON.parse(jobData.opts);
            } catch (e) {
                console.warn(`Could not parse job options for ${jobId}`);
            }
        }

        return jobData;
    }

    /**
     * Obtiene trabajos retrasados que están listos para procesar
     * @returns {Promise<string[]>} IDs de trabajos listos
     */
    async getReadyDelayed() {
        const now = Date.now();
        const delayedKey = `${this.prefix}:delayed`;

        // Get jobs that are ready to be processed
        const jobs = await this.connection.pipeline()
            .zrangebyscore(delayedKey, 0, now)
            .zremrangebyscore(delayedKey, 0, now)
            .exec();

        if (!jobs || !jobs[0] || !jobs[0][1]) {
            return [];
        }

        const readyJobs = jobs[0][1];

        // Move jobs to their respective group queues
        for (const jobId of readyJobs) {
            const jobData = await this.getJob(jobId);
            if (jobData) {
                const group = jobData.group || 'default';
                await this.connection.pipeline()
                    .lpush(`${this.prefix}:groups:${group}`, jobId)
                    .hset(jobData.id, 'status', 'waiting')
                    .exec();
            }
        }

        return readyJobs;
    }

    /**
     * Obtiene el estado actual de la cola
     * @returns {Promise<Object>} Estado de la cola
     */
    async getStatus() {
        const multi = this.connection.multi();

        // Get counts for different states
        multi.scard(`${this.prefix}:groups:set`); // number of groups
        
        // Get delayed jobs count
        multi.zcard(`${this.prefix}:delayed`);

        // Get active workers
        multi.keys(`${this.prefix}:workers:*`);

        const [groupCount, delayedCount, workerKeys] = await multi.exec();

        // Get worker details
        const workers = [];
        for (const [_, key] of workerKeys) {
            const worker = await this.connection.hgetall(key);
            if (worker) {
                workers.push({
                    id: key.split(':').pop(),
                    ...worker
                });
            }
        }

        return {
            groups: groupCount[1],
            delayed: delayedCount[1],
            workers: workers.map(w => ({
                id: w.id,
                host: w.host,
                pid: parseInt(w.pid),
                concurrency: parseInt(w.concurrency),
                started: parseInt(w.started),
                lastBeat: parseInt(w.lastBeat)
            }))
        };
    }

    async close() {
        // Cleanup could be added here
    }
}

export default Queue;