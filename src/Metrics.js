import { EventEmitter } from 'events';

class Metrics extends EventEmitter {

    constructor(appName, queueName, params) {

        super();

        this.prefix = `${appName}:${queueName}`;

        this.appName = appName;
        this.queueName = queueName;
        this.connection = params.connection;

        this.metricsInterval = null;

        this.interval = params.interval || 60000; // 1 minute
        this.retention = params.retention || 24 * 60; // 24 hours

    }


    /**
   * Start the metrics collection
   */
    async startMetricsCollection() {
        if (this.metricsInterval) {
            return;
        }

        this.metricsInterval = setInterval(
            () => this.collectMetrics(),
            this.interval
        );
    }

    /**
   * Stop the metrics collection
   */
    stopMetricsCollection() {
        if (this.metricsInterval) {
            clearInterval(this.metricsInterval);
            this.metricsInterval = null;
        }
    }

    /**
     * Collect metrics from the queue
     * @private
     */
    async collectMetrics() {
        const timestamp = Date.now();
        const counts = await this.getJobCounts();

        // Store metrics in Redis
        await this.storeMetrics(timestamp, counts);

        // Emit event with metrics
        this.emit('metrics', { timestamp, counts });
    }

    /**
     * Store metrics in Redis
     * @private
     */
    async storeMetrics(timestamp, counts) {
        const multi = this.connection.multi();

        // Store counters by type
        for (const [type, count] of Object.entries(counts)) {
            const metricsKey = `${this.prefix}:metrics:${type}`;
            const dataKey = `${metricsKey}:data`;

            multi
                .hset(metricsKey, {
                    count,
                    prevTS: timestamp,
                    prevCount: count
                })
                .lpush(dataKey, JSON.stringify({ timestamp, count }))
                .ltrim(dataKey, 0, this.retention - 1);
        }

        await multi.exec();
    }

    /**
     * Get the job counters by state
     */
    async getJobCounts() {
        const counts = {
            waiting: 0,
            processing: 0,
            completed: 0,
            failed: 0
        };

        // ---- Waiting (listas por grupo) ----
        const groupSetKey = `${this.prefix}:groups:set`;
        const groups = await this.connection.smembers(groupSetKey);
        const pipeline = this.connection.pipeline();
        for (const g of groups) {
            pipeline.llen(`${this.prefix}:groups:${g}`);
        }

        const waitLens = await pipeline.exec();
        counts.waiting = waitLens.reduce((sum, [err, len]) => sum + (len || 0), 0);

        // ---- Resto de estados (hash job) ----
        let cursor = '0';
        do {
            const [nextCursor, keys] = await this.connection.scan(cursor, 'MATCH', `${this.prefix}:job:*`, 'COUNT', 1000);
            if (keys.length) {
                const statuses = await this.connection.pipeline(keys.map(k => ['hget', k, 'status'])).exec();
                statuses.forEach(([err, status]) => {
                    switch (status) {
                        case 'processing':
                            counts.processing += 1;
                            break;
                        case 'completed':
                            counts.completed += 1;
                            break;
                        case 'failed':
                            counts.failed += 1;
                            break;
                    }
                });
            }
            cursor = nextCursor;
        } while (cursor !== '0');

        return counts;
    }

    /**
   * Get historical metrics
   */
    async getMetrics(type, start = 0, end = -1) {
        const metricsKey = `${this.prefix}:metrics:${type}`;
        const dataKey = `${metricsKey}:data`;

        const multi = this.connection.multi();
        multi
            .hmget(metricsKey, 'count', 'prevTS', 'prevCount')
            .lrange(dataKey, start, end)
            .llen(dataKey);

        const [[, meta], [, data], [, total]] = await multi.exec();

        return {
            meta: {
                count: parseInt(meta[0] || '0'),
                prevTS: parseInt(meta[1] || '0'),
                prevCount: parseInt(meta[2] || '0')
            },
            data: data.map(item => JSON.parse(item)),
            total
        };
    }

    /**
   * Get performance statistics
   */
    async getPerformanceStats(timeWindow = 3600000) { // 1 hour by default
        const now = Date.now();
        const start = now - timeWindow;

        const completedMetrics = await this.getMetrics('completed');
        const failedMetrics = await this.getMetrics('failed');

        // Filter data within the time window
        const recentCompleted = completedMetrics.data
            .filter(item => item.timestamp > start);
        const recentFailed = failedMetrics.data
            .filter(item => item.timestamp > start);

        // Calculate rates
        const successRate = recentCompleted.length / (recentCompleted.length + recentFailed.length) || 0;
        const throughput = (recentCompleted.length + recentFailed.length) / (timeWindow / 1000);

        return {
            successRate,
            throughput,
            completed: recentCompleted.length,
            failed: recentFailed.length,
            timeWindow
        };
    }

    /**
     * Clean old metrics
     */
    async cleanOldMetrics(maxAge = 7 * 24 * 60 * 60 * 1000) { // 7 days by default
        const types = ['completed', 'failed'];
        const now = Date.now();

        for (const type of types) {
            const metricsKey = `${this.prefix}:metrics:${type}`;
            const dataKey = `${metricsKey}:data`;

            const data = await this.connection.lrange(dataKey, 0, -1);
            const filteredData = data
                .map(JSON.parse)
                .filter(item => now - item.timestamp <= maxAge);

            if (filteredData.length < data.length) {
                await this.connection
                    .multi()
                    .del(dataKey)
                    .lpush(dataKey, ...filteredData.map(JSON.stringify))
                    .exec();
            }
        }
    }

}

export default Metrics;