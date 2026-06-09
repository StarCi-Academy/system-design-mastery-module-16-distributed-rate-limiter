// The single, language-agnostic heart of the lesson. Loaded onto every master
// shard at startup via SCRIPT LOAD and invoked with EVALSHA. This exact Lua body
// is byte-for-byte identical across all four language implementations.
//
// KEYS[1]=user key, KEYS[2]=tenant key, KEYS[3]=global key
// ARGV[1]=now_ms (unused placeholder), ARGV[2]=window_ms
// ARGV[3]=user_limit, ARGV[4]=tenant_limit, ARGV[5]=global_limit
// Returns {allowed(1/0), blockedAt("OK"|"USER"|"TENANT"|"GLOBAL"), userCount, tenantCount, globalCount}
export const CASCADE_LUA = `
local user_key   = KEYS[1]
local tenant_key = KEYS[2]
local global_key = KEYS[3]
local window_ms    = tonumber(ARGV[2])
local user_limit   = tonumber(ARGV[3])
local tenant_limit = tonumber(ARGV[4])
local global_limit = tonumber(ARGV[5])

-- bump reads first (no blind INCR) so it never overshoots the limit.
local function bump(key, limit)
    local v = tonumber(redis.call('GET', key) or '0')
    if v >= limit then return 0, v end
    redis.call('INCR', key)
    redis.call('PEXPIRE', key, window_ms + 500)
    return 1, v + 1
end

-- Cascade: cheapest/innermost tier (user) first.
local user_ok, user_v = bump(user_key, user_limit)
if user_ok == 0 then return {0, 'USER', user_v, 0, 0} end

local tenant_ok, tenant_v = bump(tenant_key, tenant_limit)
if tenant_ok == 0 then
    redis.call('DECR', user_key)                 -- compensation: undo user INCR
    return {0, 'TENANT', user_v - 1, tenant_v, 0}
end

local global_ok, global_v = bump(global_key, global_limit)
if global_ok == 0 then
    redis.call('DECR', user_key)                 -- compensation: undo user INCR
    redis.call('DECR', tenant_key)               -- compensation: undo tenant INCR
    return {0, 'GLOBAL', user_v - 1, tenant_v - 1, global_v}
end

return {1, 'OK', user_v, tenant_v, global_v}
`
