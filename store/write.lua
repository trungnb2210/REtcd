-- write.lua — atomic unconditional PUT or DELETE of a single etcd key in ONE
-- Redis round-trip (replaces the previous GET + INCR + pipeline = 3 sequential
-- round-trips in Go). Atomicity also guarantees the event stream order matches
-- revision order, even under concurrent writers.
--
-- Stored blob format matches encodeKV (big-endian int64s):
--   bytes 1-8 create_rev, 9-16 mod_rev, 17-24 version, 25-32 lease, 33+ value
--
-- KEYS[1]=redisKey(key)  KEYS[2]=revisionKey  KEYS[3]=eventStream  KEYS[4]=keyindex
-- ARGV[1]="PUT"|"DELETE"  ARGV[2]=value (PUT)  ARGV[3]=lease_id
-- returns {new_rev_string, existing_blob_or_empty, new_blob_or_empty, stream_id_or_empty}
--   existing_blob = overwritten/deleted value ('' on create or absent delete)
--   new_blob      = the freshly written value (PUT only; '' for DELETE) — lets the
--                   Go side build the watch event in-process without re-reading it
--   stream_id     = the XADD entry ID ('' when no event was appended, i.e. an
--                   absent DELETE) — drives in-process watch dispatch + the
--                   delivery-latency metric

local function dec64(s, off)
    local v = 0
    for i = off, off + 7 do v = v * 256 + string.byte(s, i) end
    return v
end
local function enc64(v)
    v = math.floor(tonumber(v) or 0)
    local b = {}
    for i = 8, 1, -1 do b[i] = string.char(v % 256); v = math.floor(v / 256) end
    return table.concat(b)
end

local current_raw = redis.call('GET', KEYS[1])
local op       = ARGV[1]
local etcd_key = string.sub(KEYS[1], 4) -- strip "kv:"
local cur_lease = 0
if current_raw and #current_raw >= 32 then cur_lease = dec64(current_raw, 25) end

if op == 'DELETE' then
    if not current_raw then
        -- nothing to delete: don't bump the revision (matches old Go behaviour)
        return {tostring(tonumber(redis.call('GET', KEYS[2]) or '0') or 0), '', '', ''}
    end
    local new_rev = redis.call('INCR', KEYS[2])
    redis.call('DEL',  KEYS[1])
    redis.call('ZREM', KEYS[4], etcd_key)
    if cur_lease ~= 0 then redis.call('SREM', 'lease:keys:' .. cur_lease, etcd_key) end
    local sid = redis.call('XADD', KEYS[3], '*',
        'type', 'DELETE', 'key', etcd_key, 'rev', tostring(new_rev),
        'prev_data', current_raw)
    return {tostring(new_rev), current_raw, '', sid}
end

-- PUT
local new_rev = redis.call('INCR', KEYS[2])
local create_rev, version
if current_raw and #current_raw >= 32 then
    create_rev = dec64(current_raw, 1)
    version    = dec64(current_raw, 17) + 1
else
    create_rev = new_rev
    version    = 1
end
local lease = tonumber(ARGV[3]) or 0
local new_blob = enc64(create_rev) .. enc64(new_rev) .. enc64(version) .. enc64(lease) .. ARGV[2]
redis.call('SET',  KEYS[1], new_blob)
redis.call('ZADD', KEYS[4], 0, etcd_key)
if cur_lease ~= 0 and cur_lease ~= lease then redis.call('SREM', 'lease:keys:' .. cur_lease, etcd_key) end
if lease ~= 0 then redis.call('SADD', 'lease:keys:' .. lease, etcd_key) end
local sid
if current_raw then
    sid = redis.call('XADD', KEYS[3], '*',
        'type', 'PUT', 'key', etcd_key, 'rev', tostring(new_rev),
        'data', new_blob, 'prev_data', current_raw)
else
    sid = redis.call('XADD', KEYS[3], '*',
        'type', 'PUT', 'key', etcd_key, 'rev', tostring(new_rev),
        'data', new_blob)
end
return {tostring(new_rev), current_raw or '', new_blob, sid}
