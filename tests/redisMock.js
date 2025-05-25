class RedisMock {
    constructor() {
        this.data = new Map();
        this.streams = new Map();
        this.counters = new Map();
        this.lists = new Map();
    }

    async hset(key, ...args) {
        if (!this.data.has(key)) {
            this.data.set(key, new Map());
        }
        
        for (let i = 0; i < args.length; i += 2) {
            this.data.get(key).set(args[i], args[i + 1]);
        }
        
        return args.length / 2;
    }

    async hgetall(key) {
        const result = this.data.get(key);
        if (!result) return null;
        
        const obj = {};
        for (const [field, value] of result.entries()) {
            obj[field] = value;
        }
        return obj;
    }

    async eval(script, numKeys, ...args) {
        // Simulate the basic behavior of the Lua script
        // The format received is: KEYS..., ARGV...
        //   args[0] - waitKey
        //   args[1] - metaKey (not used directly)
        //   args[2] - counterKey
        //   args[3] - eventsKey (not used directly)
        //   args[4] - jobName
        //   args[5] - jobData (JSON string)
        //   args[6] - jobOptions (JSON string, not used)
        //   args[7] - timestamp

        if (script.includes("INCR")) {
            const waitKey = args[0];
            const counterKey = args[2];

            const jobName = args[4];
            const jobDataStr = args[5];
            const timestamp = args[7];

            // Generate a unique ID for the job
            const currentValue = (this.counters.get(counterKey) || 0) + 1;
            this.counters.set(counterKey, currentValue);

            const jobId = currentValue.toString();
            const jobKey = `job:${jobId}`;

            // Save the job data
            await this.hset(
                jobKey,
                'id', jobId,
                'name', jobName,
                'data', jobDataStr,
                'timestamp', timestamp,
                'status', 'waiting'
            );

            // Add to the waiting list
            if (!this.lists.has(waitKey)) {
                this.lists.set(waitKey, []);
            }
            this.lists.get(waitKey).unshift(jobId);

            return jobId;
        }
        return "1";
    }

    async lpush(key, value) {
        if (!this.lists.has(key)) {
            this.lists.set(key, []);
        }
        this.lists.get(key).unshift(value);
        return this.lists.get(key).length;
    }

    async rpop(key, count = 1) {
        if (!this.lists.has(key)) {
            return null;
        }
        const list = this.lists.get(key);
        if (count === 1) {
            return list.length > 0 ? list.pop() : null;
        }
        const result = [];
        for (let i = 0; i < count && list.length > 0; i++) {
            result.push(list.pop());
        }
        return result;
    }

    async xadd(stream, id, ...args) {
        if (!this.streams.has(stream)) {
            this.streams.set(stream, []);
        }
        const entry = {
            id: id === '*' ? Date.now().toString() : id,
            fields: {}
        };
        for (let i = 0; i < args.length; i += 2) {
            entry.fields[args[i]] = args[i + 1];
        }
        this.streams.get(stream).push(entry);
        return entry.id;
    }

    pipeline() {
        return new RedisPipelineMock(this);
    }
}

class RedisPipelineMock {
    constructor(redisMock) {
        this.redisMock = redisMock;
        this.commands = [];
    }

    rpop(key, count) {
        this.commands.push(['rpop', key, count]);
        return this;
    }

    hset(key, ...args) {
        this.commands.push(['hset', key, ...args]);
        return this;
    }

    xadd(stream, id, ...args) {
        this.commands.push(['xadd', stream, id, ...args]);
        return this;
    }

    async exec() {
        const results = [];
        for (const [cmd, ...args] of this.commands) {
            try {
                const result = await this.redisMock[cmd](...args);
                results.push([null, result]);
            } catch (error) {
                results.push([error, null]);
            }
        }
        this.commands = [];
        return results;
    }
}

export default RedisMock; 