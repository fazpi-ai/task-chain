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

    async add(jobName, data) {

        const timestamp = Date.now();

        // Determine group
        let group = 'default';
        if (data && typeof data === 'object' && data.group && typeof data.group === 'object' && data.group.id) {
            group = data.group.id;
        }

        // Keys that will be used by the Lua script
        const keys = [
            `${this.prefix}:wait`,             // Waiting queue (legacy / compat)
            `${this.prefix}:meta`,             // Metadata
            `${this.prefix}:counter`,          // ID counter
            `${this.prefix}:events`,           // Events stream
            `${this.prefix}:groups:${group}`,  // Group specific queue
            `${this.prefix}:groups:set`,       // Groups set
        ];

        // Arguments for the script
        const args = [
            jobName,                    // Job name
            JSON.stringify(data),       // Job data
            JSON.stringify({}),         // Job options (placeholder)
            timestamp.toString(),       // Timestamp
            this.prefix,                // Prefix for other keys
            group                       // Group name
        ];

        try {
            // Load and execute the script
            const result = await this.connection.eval(
                this.addJobScript,
                keys.length,
                ...keys,
                ...args
            );

            return result.toString();
        } catch (error) {
            console.error('Error adding job:', error);
            throw error;
        }

    }

    async close() {

    }

}

export default Queue;