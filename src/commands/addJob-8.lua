--[[
  Script to add a job to the queue
  
  KEYS[1] - Waiting queue (wait)
  KEYS[2] - Metadata key (meta)
  KEYS[3] - ID key (counter)
  KEYS[4] - Events key (events)
  KEYS[5] - Group specific queue (groups:{group})
  KEYS[6] - Groups set (groups:set)
  
  ARGV[1] - Job name
  ARGV[2] - Job data (JSON)
  ARGV[3] - Job options (JSON)
  ARGV[4] - Timestamp
  ARGV[5] - Queue prefix (e.g. "FAZPIAI:EMAILS")
  ARGV[6] - Group name
]]

local waitKey = KEYS[1]
local metaKey = KEYS[2]
local counterKey = KEYS[3]
local eventsKey = KEYS[4]
local groupKey = KEYS[5]
local groupsSet = KEYS[6]

-- Generate a unique ID for the job
local jobId = redis.call("INCR", counterKey)
local prefix = ARGV[5]
local group = ARGV[6]
local jobKey = prefix .. ":job:" .. jobId

-- Save the job
local jobData = {
    id = jobId,
    name = ARGV[1],
    data = ARGV[2],
    timestamp = ARGV[4],
    status = "waiting",
    group = group
}

-- Save the job
redis.call("HSET", jobKey, 
    "id", jobId,
    "name", ARGV[1],
    "data", ARGV[2],
    "timestamp", ARGV[4],
    "status", "waiting",
    "group", group
)

-- Register group and add to the group queue
redis.call("SADD", groupsSet, group)
redis.call("LPUSH", groupKey, jobId)

-- Emit the event
redis.call("XADD", eventsKey, "*", 
    "event", "waiting",
    "jobId", jobId,
    "group", group
)

return jobId