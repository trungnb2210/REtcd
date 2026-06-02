-- txn.lua — atomic compare-and-swap for a single etcd key.
--
-- Stored blob format (binary, big-endian):
--   bytes  1- 8  create_revision (int64)
--   bytes  9-16  mod_revision    (int64)
--   bytes 17-24  version         (int64)
--   bytes 25-32  lease           (int64)
--   bytes 33+    raw value bytes
--
-- Returns {succeeded, current_blob_or_empty, revision_string}
--   succeeded = 1 (compare passed, write done) or 0 (compare failed)
--   current_blob_or_empty = existing binary blob on failure, '' on success
--   revision_string = new global revision on success, current on failure
--
-- KEYS[1] = redisKey(key)   e.g. "kv:/registry/pods/default/mypod"
-- KEYS[2] = revisionKey     "global:revision"
-- KEYS[3] = eventStream     "events"
-- KEYS[4] = "keyindex"
-- ARGV[1] = expected mod_revision (-1 means key must not exist)
-- ARGV[2] = raw value bytes for PUT
-- ARGV[3] = "PUT" or "DELETE"
-- ARGV[4] = lease_id

local function dec64(s, off)
    local v = 0
    for i = off, off + 7 do
        v = v * 256 + string.byte(s, i)
    end
    return v
end

local function enc64(v)
    v = math.floor(tonumber(v) or 0)
    local b = {}
    for i = 8, 1, -1 do
        b[i] = string.char(v % 256)
        v = math.floor(v / 256)
    end
    return table.concat(b)
end

-- Fetch global revision upfront so we can return it on compare failure.
local cur_global_rev_num = tonumber(redis.call('GET', KEYS[2]) or '0') or 0
if cur_global_rev_num < 1 then cur_global_rev_num = 1 end
local cur_global_rev = tostring(cur_global_rev_num)

local current_raw = redis.call('GET', KEYS[1])

local current_mod_rev    = 0
local current_create_rev = 0
local current_version    = 0
if current_raw and #current_raw >= 32 then
    current_create_rev = dec64(current_raw,  1)
    current_mod_rev    = dec64(current_raw,  9)
    current_version    = dec64(current_raw, 17)
end

local expected = tonumber(ARGV[1])

-- Compare: -1 means "key must not exist", otherwise match mod_revision.
if expected == -1 then
    if current_raw then
        return {0, current_raw, cur_global_rev}
    end
elseif current_mod_rev ~= expected then
    return {0, current_raw or '', cur_global_rev}
end

-- Compare passed — perform the write.
local new_rev  = redis.call('INCR', KEYS[2])
local op       = ARGV[3]
local etcd_key = string.sub(KEYS[1], 4) -- strip "kv:" prefix

if op == 'DELETE' then
    redis.call('DEL',  KEYS[1])
    redis.call('ZREM', KEYS[4], etcd_key)
    -- prev_data (the deleted object) is required: the apiserver's WithPrevKV
    -- watch rejects a DELETE event with PrevKv=nil and tears down its cache.
    redis.call('XADD', KEYS[3], '*',
        'type', 'DELETE',
        'key',  etcd_key,
        'rev',  tostring(new_rev),
        'prev_data', current_raw)
else
    local create_rev = current_raw and current_create_rev or new_rev
    local version    = current_raw and (current_version + 1) or 1
    local lease      = tonumber(ARGV[4]) or 0

    local new_blob = enc64(create_rev) .. enc64(new_rev) .. enc64(version) .. enc64(lease) .. ARGV[2]
    redis.call('SET',  KEYS[1], new_blob)
    redis.call('ZADD', KEYS[4], 0, etcd_key)

    if current_raw then
        redis.call('XADD', KEYS[3], '*',
            'type',      'PUT',
            'key',       etcd_key,
            'rev',       tostring(new_rev),
            'data',      new_blob,
            'prev_data', current_raw)
    else
        redis.call('XADD', KEYS[3], '*',
            'type', 'PUT',
            'key',  etcd_key,
            'rev',  tostring(new_rev),
            'data', new_blob)
    end
end

return {1, '', tostring(new_rev)}
