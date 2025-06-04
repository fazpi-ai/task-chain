import { readFileSync } from 'fs';
import { fileURLToPath } from 'url';
import { dirname, join } from 'path';
import { EventEmitter } from 'events';

// Getting __dirname in ES modules
const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

class Queue extends EventEmitter {

    constructor(appName, queueName, params) {
        super();
        this.prefix = `${appName}:${queueName}`;
        this.appName = appName;
        this.queueName = queueName;

        if (!params.connection) {
            throw new Error('A valid Redis connection is required');
        }
        this.connection = params.connection;

        // Load Lua scripts
        this.scripts = {
            addJob: readFileSync(join(__dirname, 'commands', 'addJob-8.lua'), 'utf8'),
            retryJob: readFileSync(join(__dirname, 'commands', 'retryJob-1.lua'), 'utf8')
        };

        // Nuevos conjuntos para trabajos fallidos y completados
        this.failedSet = `${this.prefix}:failed`;
        this.completedSet = `${this.prefix}:completed`;
        this.delayedSet = `${this.prefix}:delayed`;

        // Conjuntos para prioridades
        this.priorityPrefix = `${this.prefix}:priority`;

        // Rate limiting
        this.rateLimitKey = `${this.prefix}:ratelimit`;
        this.defaultRateLimit = params.rateLimit || {
            max: 0,      // Sin límite por defecto
            duration: 0  // Sin límite por defecto
        };

        // Seguimiento de trabajos
        this.jobSubscriptions = new Map();
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
     * Añade un trabajo a la cola con soporte de prioridades
     * @param {string} jobName - Nombre del trabajo
     * @param {Object} data - Datos del trabajo
     * @param {Object} [opts={}] - Opciones del trabajo
     * @returns {Promise<string>} ID del trabajo
     */
    async add(jobName, data, opts = {}) {
        // Verificar rate limit global
        const [isLimited, ttl] = await this.isRateLimited();
        if (isLimited) {
            throw new RateLimitError(`Rate limit reached. Try again in ${ttl}ms`);
        }

        // Verificar rate limit de grupo
        if (data?.group?.id) {
            const [isGroupLimited, groupTtl] = await this.isRateLimited(data.group.id);
            if (isGroupLimited) {
                throw new RateLimitError(`Group rate limit reached. Try again in ${groupTtl}ms`);
            }
        }

        // Verificar límites de rate configurados
        const rateLimit = opts.rateLimit || this.defaultRateLimit;
        if (rateLimit.max > 0 && rateLimit.duration > 0) {
            const window = Math.floor(Date.now() / rateLimit.duration);
            const rateKey = `${this.rateLimitKey}:${window}`;
            
            const current = await this.connection.incr(rateKey);
            if (current === 1) {
                await this.connection.pexpire(rateKey, rateLimit.duration);
            }

            if (current > rateLimit.max) {
                throw new RateLimitError(`Rate limit of ${rateLimit.max} jobs per ${rateLimit.duration}ms exceeded`);
            }
        }

        const timestamp = Date.now();
        const priority = opts.priority || 0;

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
            priority,
            ...opts
        };

        // Keys para el script Lua
        const keys = [
            `${this.prefix}:wait`,             // Cola de espera
            `${this.prefix}:meta`,             // Metadatos
            `${this.prefix}:counter`,          // Contador de IDs
            `${this.prefix}:events`,           // Stream de eventos
            `${this.prefix}:groups:${group}`,  // Cola del grupo
            `${this.prefix}:groups:set`,       // Set de grupos
            `${this.prefix}:delayed`,          // Trabajos retrasados
            `${this.priorityPrefix}:${group}`  // Cola de prioridad del grupo
        ];

        // Argumentos para el script
        const args = [
            jobName,                           // Nombre del trabajo
            JSON.stringify(data),              // Datos del trabajo
            JSON.stringify(jobOptions),        // Opciones del trabajo
            timestamp.toString(),              // Timestamp
            this.prefix,                       // Prefijo para otras claves
            group,                             // Nombre del grupo
            priority.toString()                // Prioridad
        ];

        try {
            const jobId = await this.connection.eval(
                this.scripts.addJob,
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

    /**
     * Reintentar trabajos fallidos o completados
     * @param {Object} opts Opciones de reintento
     * @param {number} [opts.count=1] Número de trabajos a reintentar por iteración
     * @param {string} [opts.state='failed'] Estado de los trabajos a reintentar ('failed' o 'completed')
     * @param {number} [opts.timestamp] Timestamp desde el cual reintentar trabajos
     * @returns {Promise<number>} Número de trabajos reintentados
     */
    async retryJobs(opts = {}) {
        const {
            count = 1,
            state = 'failed',
            timestamp = Date.now()
        } = opts;

        const sourceSet = state === 'completed' ? this.completedSet : this.failedSet;
        let retriedCount = 0;

        // Obtener trabajos hasta el timestamp
        const jobs = await this.connection.zrangebyscore(
            sourceSet,
            0,
            timestamp,
            'LIMIT',
            0,
            count
        );

        for (const jobId of jobs) {
            const jobKey = `${this.prefix}:job:${jobId}`;
            const jobData = await this.getJob(jobId);

            if (!jobData) continue;

            // Determinar el grupo
            const group = jobData.data?.group?.id || 'default';
            const groupQueue = `${this.prefix}:groups:${group}`;

            try {
                // Ejecutar script Lua de reintento
                const result = await this.connection.eval(
                    this.scripts.retryJob,
                    5, // número de KEYS
                    sourceSet,           // KEYS[1] source set
                    jobKey,             // KEYS[2] job key
                    groupQueue,         // KEYS[3] group queue
                    `${this.prefix}:groups:set`, // KEYS[4] groups set
                    `${this.prefix}:events`,     // KEYS[5] events stream
                    jobId,              // ARGV[1] job ID
                    group,              // ARGV[2] group
                    Date.now().toString() // ARGV[3] timestamp
                );

                if (result === 1) {
                    retriedCount++;
                    this.emit('waiting', { 
                        jobId, 
                        previousState: state,
                        group 
                    });
                }
            } catch (error) {
                console.error(`Error retrying job ${jobId}:`, error);
            }
        }

        return retriedCount;
    }

    /**
     * Reintentar un trabajo específico
     * @param {string} jobId ID del trabajo a reintentar
     * @returns {Promise<boolean>} true si el reintento fue exitoso
     */
    async retryJob(jobId) {
        const jobKey = `${this.prefix}:job:${jobId}`;
        const jobData = await this.getJob(jobId);

        if (!jobData) {
            throw new Error(`Job ${jobId} not found`);
        }

        if (jobData.status !== 'failed' && jobData.status !== 'completed') {
            throw new Error(`Cannot retry job ${jobId} in state: ${jobData.status}`);
        }

        const sourceSet = jobData.status === 'completed' ? this.completedSet : this.failedSet;
        const group = jobData.data?.group?.id || 'default';
        const groupQueue = `${this.prefix}:groups:${group}`;

        try {
            const result = await this.connection.eval(
                this.scripts.retryJob,
                5,
                sourceSet,
                jobKey,
                groupQueue,
                `${this.prefix}:groups:set`,
                `${this.prefix}:events`,
                jobId,
                group,
                Date.now().toString()
            );

            if (result === 1) {
                this.emit('waiting', { 
                    jobId, 
                    previousState: jobData.status,
                    group 
                });
                return true;
            }
            return false;
        } catch (error) {
            console.error(`Error retrying job ${jobId}:`, error);
            throw error;
        }
    }

    /**
     * Obtener conteo de trabajos por estado
     * @param {...string} states Estados a contar
     * @returns {Promise<Object>} Conteo por estado
     */
    async getJobCounts(...states) {
        const pipeline = this.connection.pipeline();
        const counts = {};

        for (const state of states) {
            switch (state) {
                case 'failed':
                    pipeline.zcard(this.failedSet);
                    break;
                case 'completed':
                    pipeline.zcard(this.completedSet);
                    break;
                case 'delayed':
                    pipeline.zcard(`${this.prefix}:delayed`);
                    break;
                case 'waiting':
                    pipeline.scard(`${this.prefix}:groups:set`);
                    break;
            }
        }

        const results = await pipeline.exec();
        states.forEach((state, index) => {
            counts[state] = results[index][1];
        });

        return counts;
    }

    /**
     * Limpiar trabajos antiguos
     * @param {number} grace Tiempo en ms antes del cual eliminar trabajos
     * @param {string} state Estado de los trabajos a limpiar
     * @returns {Promise<number>} Número de trabajos eliminados
     */
    async clean(grace, state = 'completed') {
        const maxScore = Date.now() - grace;
        const set = state === 'completed' ? this.completedSet : this.failedSet;

        // Obtener trabajos a eliminar
        const jobs = await this.connection.zrangebyscore(set, 0, maxScore);

        if (jobs.length === 0) return 0;

        // Eliminar trabajos y sus datos
        const pipeline = this.connection.pipeline();

        for (const jobId of jobs) {
            pipeline.del(`${this.prefix}:job:${jobId}`);
            pipeline.zrem(set, jobId);
        }

        await pipeline.exec();
        return jobs.length;
    }

    /**
     * Obtener trabajos fallidos
     * @param {number} start Inicio del rango
     * @param {number} end Fin del rango
     * @returns {Promise<Array>} Lista de trabajos fallidos
     */
    async getFailedJobs(start = 0, end = -1) {
        const jobIds = await this.connection.zrange(this.failedSet, start, end);
        const jobs = [];

        for (const jobId of jobIds) {
            const job = await this.getJob(jobId);
            if (job) jobs.push(job);
        }

        return jobs;
    }

    /**
     * Obtiene el siguiente trabajo respetando prioridades
     * @param {string} group Grupo del cual obtener el trabajo
     * @returns {Promise<string|null>} ID del trabajo o null si no hay trabajos
     */
    async getNextJob(group = 'default') {
        const priorityKey = `${this.priorityPrefix}:${group}`;
        const groupKey = `${this.prefix}:groups:${group}`;

        // Primero intentar obtener un trabajo de la cola de prioridad
        let jobId = await this.connection.rpop(priorityKey);
        
        // Si no hay trabajos prioritarios, obtener de la cola normal
        if (!jobId) {
            jobId = await this.connection.rpop(groupKey);
        }

        return jobId;
    }

    /**
     * Aplica rate limit a un trabajo o grupo
     * @param {number} duration Duración en ms del rate limit
     * @param {string} [group] Grupo al que aplicar el rate limit (opcional)
     * @returns {Promise<void>}
     */
    async rateLimit(duration, group = null) {
        const now = Date.now();
        const key = group 
            ? `${this.rateLimitKey}:${group}`
            : this.rateLimitKey;

        await this.connection.pipeline()
            .set(key, now + duration, 'PX', duration)
            .exec();
    }

    /**
     * Verifica si un trabajo o grupo está rate limited
     * @param {string} [group] Grupo a verificar (opcional)
     * @returns {Promise<[boolean, number]>} [está limitado, tiempo restante en ms]
     */
    async isRateLimited(group = null) {
        const key = group 
            ? `${this.rateLimitKey}:${group}`
            : this.rateLimitKey;

        const result = await this.connection.get(key);
        if (!result) return [false, 0];

        const ttl = parseInt(result) - Date.now();
        return [ttl > 0, Math.max(0, ttl)];
    }

    /**
     * Suscribirse a eventos de un trabajo específico
     * @param {string} jobId ID del trabajo
     * @param {Object} callbacks Callbacks para diferentes eventos
     * @param {Function} callbacks.onCompleted Callback cuando el trabajo se completa
     * @param {Function} callbacks.onFailed Callback cuando el trabajo falla
     * @param {Function} callbacks.onRetry Callback cuando el trabajo se reintenta
     * @returns {Function} Función para cancelar la suscripción
     */
    async subscribeToJob(jobId, callbacks) {
        this.jobSubscriptions.set(jobId, callbacks);

        // Verificar estado actual del trabajo
        const job = await this.getJob(jobId);
        if (job) {
            switch (job.status) {
                case 'completed':
                    callbacks.onCompleted?.(job);
                    break;
                case 'failed':
                    callbacks.onFailed?.(job, new Error(job.failedReason));
                    break;
            }
        }

        // Retornar función para cancelar suscripción
        return () => {
            this.jobSubscriptions.delete(jobId);
        };
    }

    /**
     * Añadir trabajo con seguimiento
     * @param {string} jobName Nombre del trabajo
     * @param {Object} data Datos del trabajo
     * @param {Object} opts Opciones del trabajo
     * @returns {Promise<{jobId: string, promise: Promise}>} ID del trabajo y promesa de finalización
     */
    async addWithPromise(jobName, data, opts = {}) {
        const jobId = await this.add(jobName, data, opts);

        const promise = new Promise((resolve, reject) => {
            this.subscribeToJob(jobId, {
                onCompleted: (job) => resolve({ status: 'completed', job }),
                onFailed: (job, error) => {
                    // Solo rechazar si no hay más reintentos
                    if (job.attemptsMade >= job.opts.attempts) {
                        reject({ status: 'failed', job, error });
                    }
                },
                onRetry: (job, error) => {
                    // Emitir evento de reintento pero no resolver/rechazar la promesa
                    this.emit('job:retry', { jobId, error, nextAttempt: job.attemptsMade + 1 });
                }
            });
        });

        return { jobId, promise };
    }

    // Sobrescribir método _handleJobCompletion para notificar a suscriptores
    async _handleJobCompletion(jobId, status, result) {
        const callbacks = this.jobSubscriptions.get(jobId);
        if (callbacks) {
            const job = await this.getJob(jobId);
            if (!job) return;

            switch (status) {
                case 'completed':
                    callbacks.onCompleted?.(job);
                    break;
                case 'failed':
                    if (job.attemptsMade < job.opts.attempts) {
                        callbacks.onRetry?.(job, result);
                    } else {
                        callbacks.onFailed?.(job, result);
                    }
                    break;
            }
        }
    }
}

export default Queue;