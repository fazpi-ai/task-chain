--[[
Input:
    KEYS[1] source set (failed/completed)
    KEYS[2] job key
    KEYS[3] group queue
    KEYS[4] groups set
    KEYS[5] events stream

    ARGV[1] job ID
    ARGV[2] group
    ARGV[3] timestamp
]]

-- Check if job exists
if redis.call("EXISTS", KEYS[2]) == 0 then
    return -1
end

-- Remove from source set
redis.call("ZREM", KEYS[1], ARGV[1])

-- Reset job state
redis.call("HMSET", KEYS[2],
    "status", "waiting",
    "failedReason", "",
    "processedOn", "",
    "finishedOn", "",
    "returnvalue", ""
)

-- Ensure group exists in groups set
redis.call("SADD", KEYS[4], ARGV[2])

-- Add to group queue
redis.call("LPUSH", KEYS[3], ARGV[1])

-- Emit event
redis.call("XADD", KEYS[5], "*",
    "event", "waiting",
    "jobId", ARGV[1],
    "prev", "retry",
    "timestamp", ARGV[3]
)

return 1 