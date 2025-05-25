import { jest } from '@jest/globals';
import Queue from '../src/Queue.js';
import Worker from '../src/Worker.js';
import RedisMock from './redisMock.js';

describe('Queue y Worker Tests', () => {
    let queue;
    let worker;
    let redisMock;
    
    beforeEach(() => {
        redisMock = new RedisMock();
        queue = new Queue('testApp', 'testQueue', { connection: redisMock });
    });

    afterEach(async () => {
        if (worker) {
            worker.stop();
        }
        jest.clearAllMocks();
    });

    describe('Queue Tests', () => {
        test('should add a job to the queue correctly', async () => {
            const jobName = 'testJob';
            const jobData = { test: 'data' };
            
            const jobId = await queue.add(jobName, jobData);
            
            expect(jobId).toBeTruthy();
            expect(typeof jobId).toBe('string');
        });

        test('should handle errors when adding jobs', async () => {
            const jobName = 'testJob';
            const jobData = { test: 'data' };
            
            // Simulate a Redis error
            redisMock.eval = jest.fn().mockRejectedValue(new Error('Redis Error'));
            
            await expect(queue.add(jobName, jobData)).rejects.toThrow('Redis Error');
        });
    });

    describe('Worker Tests', () => {
        test('should process jobs correctly', async () => {
            const processJob = jest.fn().mockResolvedValue(true);
            worker = new Worker('testApp', 'testQueue', processJob, {
                connection: redisMock,
                pollInterval: 100
            });

            // Add a job to the queue
            const jobId = await queue.add('testJob', { data: 'test' });

            // Start the worker
            const workerPromise = worker.start();
            
            // Wait for the job to be processed
            await new Promise(resolve => setTimeout(resolve, 200));
            
            // Stop the worker
            worker.stop();
            
            // Verify that the job was processed
            expect(processJob).toHaveBeenCalledWith(expect.objectContaining({
                id: expect.any(String),
                name: 'testJob',
                data: { data: 'test' }
            }));

            // Verify the job status
            const jobKey = `job:${jobId}`;
            const jobData = await redisMock.hgetall(jobKey);
            expect(jobData.status).toBe('completed');
        });

        test('should handle errors in job processing', async () => {
            const processJob = jest.fn().mockRejectedValue(new Error('Job Error'));
            worker = new Worker('testApp', 'testQueue', processJob, {
                connection: redisMock,
                pollInterval: 100
            });

            // Add a job to the queue
            const jobId = await queue.add('testJob', { data: 'test' });

            // Start the worker
            const workerPromise = worker.start();
            
            // Wait for the job to be processed
            await new Promise(resolve => setTimeout(resolve, 200));
            
            // Stop the worker
            worker.stop();
            
            // Verify that the job was marked as failed
            const jobKey = `job:${jobId}`;
            const jobData = await redisMock.hgetall(jobKey);
            expect(jobData.status).toBe('failed');
            expect(jobData.error).toBe('Job Error');
        });

        test('should process multiple jobs in batch', async () => {
            const processJob = jest.fn().mockResolvedValue(true);
            worker = new Worker('testApp', 'testQueue', processJob, {
                connection: redisMock,
                pollInterval: 100,
                batchSize: 3
            });

            // Add multiple jobs to the queue
            const jobPromises = [
                queue.add('job1', { data: '1' }),
                queue.add('job2', { data: '2' }),
                queue.add('job3', { data: '3' })
            ];

            const jobIds = await Promise.all(jobPromises);

            // Start the worker
            const workerPromise = worker.start();
            
            // Wait for the jobs to be processed
            await new Promise(resolve => setTimeout(resolve, 200));
            
            // Stop the worker
            worker.stop();
            
            // Verify that all jobs were processed
            expect(processJob).toHaveBeenCalledTimes(3);

            // Verify the status of all jobs
            for (const jobId of jobIds) {
                const jobKey = `job:${jobId}`;
                const jobData = await redisMock.hgetall(jobKey);
                expect(jobData.status).toBe('completed');
            }
        });
    });
}); 