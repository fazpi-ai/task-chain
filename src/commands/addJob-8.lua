--[[
  Script to add a job to the queue
  
  KEYS[1] - Waiting queue (wait)
  KEYS[2] - Metadata key (meta)
  KEYS[3] - ID key (counter)
  KEYS[4] - Events key (events)
  KEYS[5] - Group specific queue (groups:{group})
  KEYS[6] - Groups set (groups:set)
  KEYS[7] - Delayed jobs sorted set (delayed)
  KEYS[8] - Priority queue (priority)
  
  ARGV[1] - Job name
  ARGV[2] - Job data (JSON)
  ARGV[3] - Job options (JSON)
  ARGV[4] - Timestamp
  ARGV[5] - Queue prefix (e.g. "FAZPIAI:EMAILS")
  ARGV[6] - Group name
  ARGV[7] - Priority
]]

local waitKey = KEYS[1]
local metaKey = KEYS[2]
local counterKey = KEYS[3]
local eventsKey = KEYS[4]
local groupKey = KEYS[5]
local groupsSet = KEYS[6]
local delayedKey = KEYS[7]
local priorityKey = KEYS[8]

-- Generate a unique ID for the job
local jobId = redis.call("INCR", counterKey)
local prefix = ARGV[5]
local group = ARGV[6]
local jobKey = prefix .. ":job:" .. jobId

-- Parse options
local opts = cjson.decode(ARGV[3])
local delay = tonumber(opts["delay"] or "0")
local timestamp = tonumber(ARGV[4])
local priority = tonumber(ARGV[7] or "0")

-- Save the job
local jobData = {
    id = jobId,
    name = ARGV[1],
    data = ARGV[2],
    timestamp = ARGV[4],
    status = delay > 0 and "delayed" or "waiting",
    group = group,
    opts = ARGV[3],
    attempts = "0",
    maxAttempts = opts["attempts"] or "3"
}

-- Save the job atomically
redis.call("HSET", jobKey, 
    "id", jobId,
    "name", ARGV[1],
    "data", ARGV[2],
    "timestamp", ARGV[4],
    "status", jobData.status,
    "group", group,
    "opts", ARGV[3],
    "attempts", "0",
    "maxAttempts", jobData.maxAttempts
)

-- Register group
redis.call("SADD", groupsSet, group)

-- Add to appropriate queue
if delay > 0 then
    -- Add to delayed queue with timestamp + delay
    local processAt = timestamp + delay
    redis.call("ZADD", delayedKey, processAt, jobId)
    redis.call("HSET", jobKey, "status", "delayed", "processAt", processAt)
    
    -- Emit delayed event
    redis.call("XADD", eventsKey, "*", 
        "event", "delayed",
        "jobId", jobId,
        "group", group,
        "delay", delay,
        "priority", priority
    )
else
    if priority > 0 then
        -- Add to priority queue
        redis.call("LPUSH", priorityKey, jobId)
    else
        -- Add to group queue
        redis.call("LPUSH", groupKey, jobId)
    end
    
    -- Emit waiting event
    redis.call("XADD", eventsKey, "*", 
        "event", "waiting",
        "jobId", jobId,
        "group", group,
        "priority", priority
    )
end

-- Update metadata
redis.call("HINCRBY", metaKey, "total", 1)
if priority > 0 then
    redis.call("HINCRBY", metaKey, "priority", 1)
end

return jobId