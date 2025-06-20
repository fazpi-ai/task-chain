# @fazpi-ai/task-chain
 Powerful distributed processing library based on Redis Streams, designed to build parallel, ordered, and reliable workflows.

// Configuración global
const queue = new Queue('myApp', 'myQueue', {
    connection,
    rateLimit: {
        max: 1000,
        duration: 60000
    }
});

// Rate limit específico para un trabajo
await queue.add('api-call', data, {
    rateLimit: {
        max: 10,         // Máximo 10 llamadas
        duration: 1000   // Por segundo
    }
});

// Rate limit manual
await queue.rateLimit(5000);              // Global
await queue.rateLimit(5000, 'api-group'); // Por grupo

const worker = new Worker('myApp', 'myQueue', async (job) => {
    try {
        if (needsRateLimit(job)) {
            await queue.rateLimit(5000, job.data.group?.id);
            throw new RateLimitError();
        }

        const result = await processJob(job);
        return result;
    } catch (error) {
        if (error instanceof RateLimitError) {
            // Se reintentará automáticamente
            throw error;
        }
        if (isUnrecoverable(error)) {
            throw new UnrecoverableError(error.message);
        }
        throw error; // Reintento normal
    }
}, {
    stalledCheckInterval: 30000,
    maxStalledCount: 3,
    backoff: {
        type: 'exponential',
        delay: 1000
    }
});

// Monitoreo
worker.on('stalled', ({ jobId, stalledCount }) => {
    console.warn(`Job ${jobId} stalled ${stalledCount} times`);
});

worker.on('failed', ({ jobId, error, stalled, willRetry }) => {
    console.error(`Job ${jobId} failed:`, error.message);
    if (stalled) {
        console.error('Failed due to stalling');
    }
    if (willRetry) {
        console.log('Will be retried');
    }
});

// Mantenimiento
setInterval(async () => {
    // Limpiar trabajos antiguos
    await queue.clean(24 * 60 * 60 * 1000, 'completed');
    await queue.clean(7 * 24 * 60 * 60 * 1000, 'failed');
}, 60 * 60 * 1000);
