import IORedis from 'ioredis';

import Queue from './src/Queue.js';
import Worker from './src/Worker.js';

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

    const jobIdp = await queue.add('SEND_EMAIL', {
        email: 'test@test.com',
        subject: 'Test',
        body: 'Test',
        group: 'example2@gmail.com'
    });

    console.log('Job IDP:', jobIdp);

    for (let i = 0; i < 2; i++) {

        const jobId = await queue.add('SEND_EMAIL', {
            email: 'test@test.com',
            subject: 'Test',
            body: 'Test',
            group: 'example@gmail.com'
        });

        console.log('Job ID:', jobId);
    }

    const jobIdp2 = await queue.add('SEND_EMAIL', {
        email: 'test@test.com',
        subject: 'Test',
        body: 'Test',
        group: 'example2@gmail.com'
    });

    console.log('Job IDP2:', jobIdp2);

    const worker = new Worker('FAZPIAI', 'EMAILS', async (job) => {
        console.log('Job:', job);
    }, {
        connection,
        concurrency: 1,
        batchSize: 2,
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

    await worker.start();

}

main();