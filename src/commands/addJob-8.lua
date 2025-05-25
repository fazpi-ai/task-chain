--[[
  Script to add a job to the queue
  
  KEYS[1] - Waiting queue (wait)
  KEYS[2] - Metadata key (meta)
  KEYS[3] - ID key (counter)
  KEYS[4] - Events key (events)
  
  ARGV[1] - Job name
  ARGV[2] - Job data (JSON)
  ARGV[3] - Job options (JSON)
  ARGV[4] - Timestamp
  ARGV[5] - Queue prefix (e.g. "FAZPIAI:EMAILS")
]]

local waitKey = KEYS[1]
local metaKey = KEYS[2]
local counterKey = KEYS[3]
local eventsKey = KEYS[4]

-- Generate a unique ID for the job
local jobId = redis.call("INCR", counterKey)
local prefix = ARGV[5]
local jobKey = prefix .. ":job:" .. jobId

-- Save the job
local jobData = {
    id = jobId,
    name = ARGV[1],
    data = ARGV[2],
    timestamp = ARGV[4],
    status = "waiting"
}

-- Save the job
redis.call("HSET", jobKey, 
    "id", jobId,
    "name", ARGV[1],
    "data", ARGV[2],
    "timestamp", ARGV[4],
    "status", "waiting"
)

-- Add to the waiting queue (FIFO)
redis.call("LPUSH", waitKey, jobId)

-- Emit the event
redis.call("XADD", eventsKey, "*", 
    "event", "waiting",
    "jobId", jobId
)

return jobId