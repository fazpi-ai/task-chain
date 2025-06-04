### 1. Manejo Mejorado de Trabajos Estancados:
```
const worker = new Worker('myApp', 'myQueue', processor, {
    stalledCheckInterval: 30000,    // Revisar cada 30 segundos
    maxStalledCount: 3,             // Máximo 3 intentos antes de fallar
    stalledTimeout: 60000           // Considerar estancado después de 1 minuto
});
```

### 2. Sistema de Prioridades:
```
// Añadir trabajo con alta prioridad
await queue.add('email', data, {
    priority: 10  // Mayor número = mayor prioridad
});

// Añadir trabajo normal
await queue.add('email', data, {
    priority: 0   // Prioridad normal
});
```

### 3. Rate Limiting Avanzado:
```
// Configuración global
const queue = new Queue('myApp', 'myQueue', {
    rateLimit: {
        max: 100,        // Máximo 100 trabajos
        duration: 60000  // Por minuto
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
```

### 4. Eventos Mejorados:
```
worker.on('stalled', ({ jobId, stalledCount, nextProcessAt }) => {
    console.log(`Job ${jobId} stalled ${stalledCount} times, next attempt at ${new Date(nextProcessAt)}`);
});

worker.on('failed', ({ jobId, error, stalled }) => {
    if (stalled) {
        console.log(`Job ${jobId} failed due to stalling too many times`);
    }
});
```

### 5. Uso Completo:
```
const queue = new Queue('myApp', 'myQueue', {
    connection,
    rateLimit: {
        max: 1000,
        duration: 60000
    }
});

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
```