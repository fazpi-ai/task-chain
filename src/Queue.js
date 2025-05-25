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

        // Keys that the Lua script will use
        const keys = [
            `${this.prefix}:wait`,    // waiting queue
            `${this.prefix}:meta`,    // Metadata
            `${this.prefix}:counter`, // ID Counter
            `${this.prefix}:events`,  // Event Stream
        ];

        // Arguments for the script
        const args = [
            jobName,                    // Job Name
            JSON.stringify(data),       // Job Data
            JSON.stringify({}),         // Options (placeholder)
            timestamp.toString(),       // Timestamp
            this.prefix,                // Prefix for other keys
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