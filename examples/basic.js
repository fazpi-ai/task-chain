import IORedis from 'ioredis';

import Queue from '../src/Queue.js';
import Worker from '../src/Worker.js';
import Metrics from '../src/Metrics.js';

const main = async () => {

    const connection = new IORedis({
        host: 'localhost',
        port: 6379
    });

    // Clean the database before starting (optional)
    await Queue.flushAll(connection);

    const queue = new Queue('FAZPIAI', 'EMAILS', {
        connection
    });

    // ----- Metrics -----
    const metrics = new Metrics('FAZPIAI', 'EMAILS', {
        connection,
        interval: 5000 // collect every 5 seconds
    });

    metrics.on('metrics', (m) => {
        console.log('Metrics:', m);
    });

    await metrics.startMetricsCollection();

    const jobIdp = await queue.add('SEND_EMAIL', {
        email: 'test@test.com',
        subject: 'Test',
        body: 'BODY PRE',
        group: { id: 'example2@gmail.com' }
    });

    console.log('Job IDP:', jobIdp);

    for (let i = 0; i < 2; i++) {

        const jobId = await queue.add('SEND_EMAIL', {
            email: 'test@test.com',
            subject: 'Test',
            body: `BODY ${i}`,
            group: { id: 'example@gmail.com' }
        });

        console.log('Job ID:', jobId);
    }

    const jobIdp2 = await queue.add('SEND_EMAIL', {
        email: 'test@test.com',
        subject: 'Test',
        body: `BODY POST`,
        group: { id: 'example2@gmail.com' }
    });

    console.log('Job IDP2:', jobIdp2);

    const worker = new Worker('FAZPIAI', 'EMAILS', async (job) => {
        console.log('Job:', job);

        // Esperamos 10 segundos
        await new Promise(resolve => setTimeout(resolve, 10000));

    }, {
        connection,
        concurrency: 2,
        batchSize: 1,
        pollInterval: 1000
    });

    worker.on('processing', (job) => {
        console.log('Processing job:', job);
    });

    worker.on('completed', (job) => {
        console.log('Completed job:', job);
    });

    worker.on('failed', (job) => {
        console.log('Failed job:', job);
    });

    // Handle signals to close cleanly
    const gracefulShutdown = async () => {
        console.log('\nShutting down...');
        await worker.stop();
        metrics.stopMetricsCollection();
        await queue.close();
        await connection.quit();
        process.exit(0);
    };

    process.on('SIGINT', gracefulShutdown);
    process.on('SIGTERM', gracefulShutdown);

    await worker.start();

}

main();