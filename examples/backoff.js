const { Worker } = require('task-chain');

/**
 * Estrategia de backoff basada en tipos de errores
 * Diferentes retrasos según el tipo de error encontrado
 */
const errorBasedBackoff = ({ attempt, error, maxDelay }) => {
    // Mapeo de errores a tiempos base de retraso (en ms)
    const errorDelays = {
        NetworkError: 5000,        // Errores de red: esperar 5s base
        DatabaseError: 10000,      // Errores de BD: esperar 10s base
        ValidationError: 1000,     // Errores de validación: esperar 1s base
        RateLimitError: 60000,     // Rate limit: esperar 1min base
        ResourceError: 30000       // Recursos no disponibles: esperar 30s base
    };

    // Obtener el retraso base según el tipo de error
    const baseDelay = errorDelays[error.name] || 3000; // default 3s

    // Aplicar incremento exponencial suave según el número de intentos
    const delay = baseDelay * Math.pow(1.5, attempt - 1);

    // Aplicar jitter (variación aleatoria) para evitar thundering herd
    const jitter = delay * (0.8 + Math.random() * 0.4); // ±20% variación

    // Respetar el límite máximo
    return Math.min(jitter, maxDelay);
};

/**
 * Estrategia de backoff basada en horarios
 * Ajusta los tiempos de reintento según la hora del día
 */
const timeBasedBackoff = ({ attempt, error, maxDelay }) => {
    const now = new Date();
    const hour = now.getHours();
    const dayOfWeek = now.getDay(); // 0 = Domingo, 1-5 = Lun-Vie, 6 = Sábado

    // Configuración de períodos
    const periods = {
        peakHours: {
            start: 9,   // 9:00 AM
            end: 18,    // 6:00 PM
            baseDelay: 1000,  // 1s en horas pico
            multiplier: 1.5   // incremento más suave
        },
        offPeakHours: {
            baseDelay: 5000,  // 5s fuera de horas pico
            multiplier: 2     // incremento más agresivo
        },
        weekend: {
            baseDelay: 10000, // 10s en fin de semana
            multiplier: 2.5   // incremento aún más agresivo
        }
    };

    // Determinar el período actual
    let config;
    if (dayOfWeek === 0 || dayOfWeek === 6) {
        config = periods.weekend;
    } else if (hour >= periods.peakHours.start && hour < periods.peakHours.end) {
        config = periods.peakHours;
    } else {
        config = periods.offPeakHours;
    }

    // Calcular el retraso base con incremento según intentos
    const delay = config.baseDelay * Math.pow(config.multiplier, attempt - 1);

    // Añadir variación aleatoria (jitter)
    const jitter = delay * (0.9 + Math.random() * 0.2); // ±10% variación

    return Math.min(jitter, maxDelay);
};

/**
 * Estrategia híbrida que combina error y tiempo
 * Considera tanto el tipo de error como el horario
 */
const hybridBackoff = ({ attempt, error, maxDelay }) => {
    // Obtener los retrasos base de ambas estrategias
    const errorDelay = errorBasedBackoff({ attempt, error, maxDelay });
    const timeDelay = timeBasedBackoff({ attempt, error, maxDelay });

    // En horas pico, priorizar la estrategia basada en errores
    const now = new Date();
    const hour = now.getHours();
    if (hour >= 9 && hour < 18) {
        return Math.min(errorDelay, maxDelay);
    }

    // Fuera de horas pico, usar el mayor de los dos retrasos
    return Math.min(Math.max(errorDelay, timeDelay), maxDelay);
};

// Ejemplo de uso con el Worker
const createWorkerWithStrategy = (strategy) => {
    return new Worker('miApp', 'miCola', async (job) => {
        // Lógica del worker...
    }, {
        connection: redisConnection, // Tu conexión a Redis
        backoff: {
            type: 'custom',
            strategy: strategy,
            maxDelay: 1000 * 60 * 30 // 30 minutos máximo
        }
    });
};

// Ejemplos de errores personalizados
class NetworkError extends Error {
    constructor(message) {
        super(message);
        this.name = 'NetworkError';
    }
}

class DatabaseError extends Error {
    constructor(message) {
        super(message);
        this.name = 'DatabaseError';
    }
}

class RateLimitError extends Error {
    constructor(message) {
        super(message);
        this.name = 'RateLimitError';
    }
}

// Ejemplos de uso
const workerWithErrorStrategy = createWorkerWithStrategy(errorBasedBackoff);
const workerWithTimeStrategy = createWorkerWithStrategy(timeBasedBackoff);
const workerWithHybridStrategy = createWorkerWithStrategy(hybridBackoff);

/**
 * Errores específicos de WhatsApp Cloud API
 * Documentación: https://developers.facebook.com/docs/whatsapp/cloud-api/support/error-codes
 */
class WhatsAppError extends Error {
    constructor(message, code, subcode = null, details = {}) {
        super(message);
        this.name = 'WhatsAppError';
        this.code = code;
        this.subcode = subcode;
        this.details = details;
    }
}

class WhatsAppRateLimitError extends WhatsAppError {
    constructor(message, code, subcode, details = {}) {
        super(message, code, subcode, details);
        this.name = 'WhatsAppRateLimitError';
    }
}

/**
 * Estrategia de backoff específica para WhatsApp Cloud API
 * Maneja diferentes tipos de rate limits:
 * - Rate limits por usuario/teléfono
 * - Rate limits por tipo de mensaje
 * - Rate limits generales de la API
 * - Ventanas de 24 horas para mensajes iniciados por negocio
 */
const whatsappBackoff = ({ attempt, error, jobData, maxDelay }) => {
    // Valores por defecto de WhatsApp Business API
    const limits = {
        DEFAULT_RETRY: 5000,        // 5 segundos
        RATE_LIMIT_USER: 60000,     // 1 minuto
        RATE_LIMIT_GLOBAL: 300000,  // 5 minutos
        TEMPLATE_LIMIT: 600000,     // 10 minutos
        WINDOW_LIMIT: 86400000      // 24 horas
    };

    // Si no es un error de WhatsApp, usar retraso por defecto
    if (!(error instanceof WhatsAppError)) {
        return Math.min(limits.DEFAULT_RETRY * attempt, maxDelay);
    }

    // Códigos de error específicos de WhatsApp
    const errorCodes = {
        RATE_LIMIT: 4,
        TEMPLATE_LIMIT: 131056,
        WINDOW_EXPIRED: 131047,
        PHONE_LIMIT: 131026
    };

    let baseDelay;

    switch (error.code) {
        case errorCodes.RATE_LIMIT:
            // Rate limit general
            baseDelay = limits.RATE_LIMIT_GLOBAL;
            break;

        case errorCodes.TEMPLATE_LIMIT:
            // Límite de plantillas alcanzado
            baseDelay = limits.TEMPLATE_LIMIT;
            break;

        case errorCodes.WINDOW_EXPIRED:
            // Ventana de 24 horas expirada
            baseDelay = limits.WINDOW_LIMIT;
            break;

        case errorCodes.PHONE_LIMIT:
            // Límite por número de teléfono
            baseDelay = limits.RATE_LIMIT_USER;
            break;

        default:
            baseDelay = limits.DEFAULT_RETRY;
    }

    // Aplicar incremento exponencial con factor 1.5
    const delay = baseDelay * Math.pow(1.5, attempt - 1);

    // Añadir jitter para evitar thundering herd
    const jitter = delay * (0.8 + Math.random() * 0.4); // ±20% variación

    return Math.min(jitter, maxDelay);
};

// Ejemplo de uso con WhatsApp Cloud API
const createWhatsAppWorker = (phoneNumberId, accessToken) => {
    return new Worker('whatsapp', 'messages', async (job) => {
        try {
            const response = await fetch(
                `https://graph.facebook.com/v17.0/${phoneNumberId}/messages`,
                {
                    method: 'POST',
                    headers: {
                        'Authorization': `Bearer ${accessToken}`,
                        'Content-Type': 'application/json'
                    },
                    body: JSON.stringify(job.data)
                }
            );

            if (!response.ok) {
                const errorData = await response.json();

                // Detectar diferentes tipos de errores
                if (errorData.error) {
                    const { message, code, error_subcode, details } = errorData.error;

                    // Detectar rate limits específicos
                    if (code === 4 || // Rate limit general
                        code === 131056 || // Template limit
                        code === 131047 || // 24h window
                        code === 131026) { // Phone number limit
                        throw new WhatsAppRateLimitError(
                            message,
                            code,
                            error_subcode,
                            details
                        );
                    }

                    // Otros errores de WhatsApp
                    throw new WhatsAppError(
                        message,
                        code,
                        error_subcode,
                        details
                    );
                }
            }

            return await response.json();
        } catch (error) {
            // Propagar el error para que el worker aplique la estrategia de backoff
            throw error;
        }
    }, {
        connection: redisConnection,
        backoff: {
            type: 'custom',
            strategy: whatsappBackoff,
            maxDelay: 1000 * 60 * 60 * 2 // 2 horas máximo
        },
        attempts: 10 // Número máximo de intentos
    });
};

// Ejemplo de uso
const sendWhatsAppMessage = async (worker, to, templateName, languageCode, components) => {
    return await worker.add({
        messaging_product: "whatsapp",
        recipient_type: "individual",
        to: to,
        type: "template",
        template: {
            name: templateName,
            language: {
                code: languageCode
            },
            components: components
        }
    });
};

/**
 * Estrategia de backoff para espaciado entre trabajos
 * Permite configurar tiempos mínimos y máximos entre ejecuciones
 */
const jobSpacingBackoff = (options = {}) => {
    const defaultOptions = {
        minDelay: 1000,        // Tiempo mínimo entre trabajos (1 segundo)
        maxDelay: 5000,        // Tiempo máximo entre trabajos (5 segundos)
        baseDelay: 2000,       // Tiempo base entre trabajos (2 segundos)
        useRandom: true,       // Si usar variación aleatoria entre min y max
        errorMultiplier: 2,    // Multiplicador en caso de error
        cooldownPeriods: {     // Períodos de enfriamiento específicos
            default: 1000,     // Delay por defecto
            high_load: 5000,   // Delay en alta carga
            maintenance: 10000  // Delay en mantenimiento
        }
    };

    // Combinar opciones por defecto con las proporcionadas
    const config = { ...defaultOptions, ...options };

    return ({ attempt, error, jobData, maxDelay }) => {
        let delay;

        // Si hay un error, aplicar multiplicador
        if (error) {
            delay = config.baseDelay * config.errorMultiplier * attempt;
        }
        // Si el trabajo tiene un período de enfriamiento específico
        else if (jobData?.cooldownPeriod && config.cooldownPeriods[jobData.cooldownPeriod]) {
            delay = config.cooldownPeriods[jobData.cooldownPeriod];
        }
        // Si se quiere variación aleatoria
        else if (config.useRandom) {
            delay = config.minDelay + Math.random() * (config.maxDelay - config.minDelay);
        }
        // Usar delay base
        else {
            delay = config.baseDelay;
        }

        // Asegurar que el delay está dentro de los límites
        return Math.min(
            Math.max(delay, config.minDelay),
            Math.min(config.maxDelay, maxDelay)
        );
    };
};

/**
 * Helper para crear un worker con espaciado entre trabajos
 */
const createSpacedWorker = (name, queue, processor, options = {}) => {
    const {
        minDelay,
        maxDelay,
        baseDelay,
        useRandom,
        errorMultiplier,
        cooldownPeriods,
        connection,
        ...workerOptions
    } = options;

    return new Worker(name, queue, processor, {
        connection,
        backoff: {
            type: 'custom',
            strategy: jobSpacingBackoff({
                minDelay,
                maxDelay,
                baseDelay,
                useRandom,
                errorMultiplier,
                cooldownPeriods
            })
        },
        ...workerOptions
    });
};

// Ejemplo de uso con diferentes configuraciones
const examples = {
    // Worker con espaciado simple
    simpleSpacing: createSpacedWorker(
        'app',
        'simple-queue',
        async (job) => {
            // Procesar trabajo
        },
        {
            connection: redisConnection,
            minDelay: 1000,    // 1 segundo mínimo
            maxDelay: 5000     // 5 segundos máximo
        }
    ),

    // Worker con espaciado variable según carga
    adaptiveSpacing: createSpacedWorker(
        'app',
        'adaptive-queue',
        async (job) => {
            // Procesar trabajo
        },
        {
            connection: redisConnection,
            minDelay: 500,     // 500ms mínimo
            maxDelay: 10000,   // 10 segundos máximo
            useRandom: true,   // Usar variación aleatoria
            cooldownPeriods: {
                low_load: 1000,      // 1s en carga baja
                medium_load: 3000,   // 3s en carga media
                high_load: 5000      // 5s en carga alta
            }
        }
    ),

    // Worker con espaciado para API rate limits
    apiSpacing: createSpacedWorker(
        'app',
        'api-queue',
        async (job) => {
            // Procesar trabajo
        },
        {
            connection: redisConnection,
            minDelay: 2000,         // 2 segundos mínimo
            maxDelay: 30000,        // 30 segundos máximo
            baseDelay: 5000,        // 5 segundos base
            errorMultiplier: 3,     // Triple de tiempo en caso de error
            cooldownPeriods: {
                rate_limited: 60000,    // 1 minuto si hay rate limit
                error: 15000           // 15 segundos si hay error
            }
        }
    )
};

// Ejemplo de uso con cooldown periods
const addJobWithCooldown = async (worker, data, cooldownPeriod = 'default') => {
    return await worker.add({
        ...data,
        cooldownPeriod // Esto será usado por la estrategia de backoff
    });
};

// Exportar las estrategias para uso en otros archivos
module.exports = {
    errorBasedBackoff,
    timeBasedBackoff,
    hybridBackoff,
    NetworkError,
    DatabaseError,
    RateLimitError,
    whatsappBackoff,
    WhatsAppError,
    WhatsAppRateLimitError,
    createWhatsAppWorker,
    sendWhatsAppMessage,
    jobSpacingBackoff,
    createSpacedWorker,
    addJobWithCooldown
};
