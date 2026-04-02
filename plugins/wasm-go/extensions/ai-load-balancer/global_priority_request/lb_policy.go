package global_priority_request

import (
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/alibaba/higress/plugins/wasm-go/extensions/ai-load-balancer/utils"
	"github.com/higress-group/proxy-wasm-go-sdk/proxywasm"
	"github.com/higress-group/proxy-wasm-go-sdk/proxywasm/types"
	"github.com/higress-group/wasm-go/pkg/log"
	"github.com/higress-group/wasm-go/pkg/wrapper"
	"github.com/tidwall/gjson"
	"github.com/tidwall/resp"
)

const (
	RedisKeyFormat          = "higress:global_priority_request_table:%s:%s"
	RedisLastCleanKeyFormat = "higress:global_priority_request_table:last_clean_time:%s:%s"

	RedisLua = `
local hset_key = KEYS[1]
local last_clean_key = KEYS[2]

local seed = tonumber(ARGV[1])
local enable_detail_log = ARGV[2]
local clean_interval = tonumber(ARGV[3])

math.randomseed(seed)

local all_hosts_total_count = 0
local all_hosts = {}

for i = 4, #ARGV, 3 do
    local level = tonumber(ARGV[i])
    local host = ARGV[i+1]
    local threshold = tonumber(ARGV[i+2])
    all_hosts[host] = {level=level, threshold=threshold}

    local val = redis.call('HGET', hset_key, host)
    if val then
        all_hosts_total_count = all_hosts_total_count + tonumber(val)
    end
end

local sorted_hosts = {}
for host, info in pairs(all_hosts) do
    table.insert(sorted_hosts, {host=host, level=info.level, threshold=info.threshold})
end
table.sort(sorted_hosts, function(a,b) return a.level < b.level end)

local selected_host = nil
for _, h in ipairs(sorted_hosts) do
    local val = tonumber(redis.call('HGET', hset_key, h.host) or 0)
    if val < h.threshold then
        selected_host = h.host
        break
    end
end

if not selected_host and #sorted_hosts > 0 then
    local idx = math.random(#sorted_hosts)
    selected_host = sorted_hosts[idx].host
end

redis.call('HINCRBY', hset_key, selected_host, 1)
local new_count = redis.call('HGET', hset_key, selected_host)

-- Step 6: 收集日志
local host_details = {}
if enable_detail_log == "1" then
    for host,_ in pairs(all_hosts) do
        local val = redis.call('HGET', hset_key, host)
        table.insert(host_details, host)
        table.insert(host_details, tostring(val or 0))
    end
end

local current_time = math.floor(seed / 1000000)
local last_clean_time = tonumber(redis.call('GET', last_clean_key) or 0)
if current_time - last_clean_time >= clean_interval then
    local all_keys = redis.call('HKEYS', hset_key)
    for _, host in ipairs(all_keys) do
        if not all_hosts[host] then
            redis.call('HDEL', hset_key, host)
        end
    end
    redis.call('SET', last_clean_key, current_time)
end

return {selected_host, new_count, host_details}
`
)

type PriorityHost struct {
	Address   string  `json:"address"`
	Threshold float64 `json:"threshold"`
}

type PriorityGroup struct {
	Level int            `json:"level"`
	Hosts []PriorityHost `json:"hosts"`
}

type GlobalPriorityRequestLoadBalancer struct {
	redisClient     wrapper.RedisClient
	priorityGroups  []PriorityGroup
	cleanInterval   int64 // seconds
	enableDetailLog bool
}

func NewGlobalPriorityRequestLoadBalancer(json gjson.Result) (GlobalPriorityRequestLoadBalancer, error) {
	lb := GlobalPriorityRequestLoadBalancer{}
	serviceFQDN := json.Get("serviceFQDN").String()
	servicePort := json.Get("servicePort").Int()
	if serviceFQDN == "" || servicePort == 0 {
		log.Errorf("invalid redis service, serviceFQDN: %s, servicePort: %d", serviceFQDN, servicePort)
		return lb, errors.New("invalid redis service config")
	}
	lb.redisClient = wrapper.NewRedisClusterClient(wrapper.FQDNCluster{
		FQDN: serviceFQDN,
		Port: servicePort,
	})
	username := json.Get("username").String()
	password := json.Get("password").String()
	timeout := json.Get("timeout").Int()
	if timeout == 0 {
		timeout = 3000
	}
	database := json.Get("database").Int()
	lb.cleanInterval = json.Get("cleanInterval").Int()
	if lb.cleanInterval == 0 {
		lb.cleanInterval = 60 * 60
	} else {
		lb.cleanInterval = lb.cleanInterval * 60
	}
	lb.enableDetailLog = true
	if val := json.Get("enableDetailLog"); val.Exists() {
		lb.enableDetailLog = val.Bool()
	}
	lb.priorityGroups = []PriorityGroup{}
	for _, pg := range json.Get("priorityGroups").Array() {
		level := int(pg.Get("level").Int())
		hostsArr := []PriorityHost{}
		for _, h := range pg.Get("hosts").Array() {
			address := h.Get("address").String()
			threshold := h.Get("threshold").Float()
			if address != "" {
				hostsArr = append(hostsArr, PriorityHost{
					Address:   address,
					Threshold: threshold,
				})
			}
		}
		if len(hostsArr) > 0 {
			lb.priorityGroups = append(lb.priorityGroups, PriorityGroup{
				Level: level,
				Hosts: hostsArr,
			})
		}
	}

	log.Errorf("redis client init, username: %s, password: %s, serviceFQDN: %s, servicePort: %d, timeout: %d, database: %d, cleanInterval: %d minutes, enableDetailLog: %v", username, password, serviceFQDN, servicePort, timeout, database, lb.cleanInterval/60, lb.enableDetailLog)

	return lb, lb.redisClient.Init(username, password, int64(timeout), wrapper.WithDataBase(int(database)))
}

func (lb GlobalPriorityRequestLoadBalancer) HandleHttpRequestHeaders(ctx wrapper.HttpContext) types.Action {
	return types.HeaderStopIteration
}

func (lb GlobalPriorityRequestLoadBalancer) HandleHttpRequestBody(ctx wrapper.HttpContext, body []byte) types.Action {
	routeName, err := utils.GetRouteName()
	if err != nil || routeName == "" {
		ctx.SetContext("error", true)
		return types.ActionContinue
	} else {
		ctx.SetContext("routeName", routeName)
	}
	clusterName, err := utils.GetClusterName()
	if err != nil || clusterName == "" {
		ctx.SetContext("error", true)
		return types.ActionContinue
	} else {
		ctx.SetContext("clusterName", clusterName)
	}

	hostInfos, err := proxywasm.GetUpstreamHosts()
	if err != nil || len(hostInfos) == 0 {
		return types.ActionContinue
	}

	healthyHostSet := make(map[string]struct{})
	for _, hostInfo := range hostInfos {
		if len(hostInfo) < 2 {
			continue
		}
		address := hostInfo[0]
		metadata := hostInfo[1]
		if gjson.Get(metadata, "health_status").String() == "Healthy" {
			healthyHostSet[address] = struct{}{}
		}
	}

	if len(healthyHostSet) == 0 {
		if lb.enableDetailLog {
			log.Infof("no healthy hosts found")
		}
		return types.ActionContinue
	}

	sort.Slice(lb.priorityGroups, func(i, j int) bool {
		return lb.priorityGroups[i].Level < lb.priorityGroups[j].Level
	})

	keys := []interface{}{
		fmt.Sprintf(RedisKeyFormat, routeName, clusterName),
		fmt.Sprintf(RedisLastCleanKeyFormat, routeName, clusterName),
	}

	enableStr := "0"
	if lb.enableDetailLog {
		enableStr = "1"
	}

	args := []interface{}{
		time.Now().UnixMicro(),
		enableStr,
		lb.cleanInterval,
	}

	for _, group := range lb.priorityGroups {
		for _, host := range group.Hosts {
			if _, ok := healthyHostSet[host.Address]; ok {
				args = append(args, group.Level, host.Address, host.Threshold)
			}
		}
	}

	err = lb.redisClient.Eval(RedisLua, len(keys), keys, args, func(response resp.Value) {
		if err := response.Error(); err != nil {
			log.Errorf("redis eval error: %+v", err)
			ctx.SetContext("error", true)
			proxywasm.ResumeHttpRequest()
			return
		}
		valArray := response.Array()
		if len(valArray) < 2 {
			log.Errorf("redis eval lua result format error: %+v", valArray)
			ctx.SetContext("error", true)
			proxywasm.ResumeHttpRequest()
			return
		}
		hostSelected := valArray[0].String()

		// detail log
		if lb.enableDetailLog && len(valArray) >= 3 {
			detailLogStr := "host and count: "
			details := valArray[2].Array()
			for i := 0; i+1 < len(details); i += 2 {
				h := details[i].String()
				c := details[i+1].String()
				detailLogStr += fmt.Sprintf("{%s: %s}, ", h, c)
			}
			log.Errorf("host_selected: %s + 1, %s", hostSelected, detailLogStr)
		}

		log.Infof("host_selected: %s", hostSelected)
		if err := proxywasm.SetUpstreamOverrideHost([]byte(hostSelected)); err != nil {
			ctx.SetContext("error", true)
			proxywasm.ResumeHttpRequest()
			return
		}
		ctx.SetContext("host_selected", hostSelected)
		proxywasm.ResumeHttpRequest()
	})
	if err != nil {
		ctx.SetContext("error", true)
		log.Errorf("redis eval failed: %+v", err)
		return types.ActionContinue
	}

	return types.ActionPause
}

func (lb GlobalPriorityRequestLoadBalancer) HandleHttpResponseHeaders(ctx wrapper.HttpContext) types.Action {
	return types.ActionContinue
}

func (lb GlobalPriorityRequestLoadBalancer) HandleHttpStreamingResponseBody(ctx wrapper.HttpContext, data []byte, endOfStream bool) []byte {
	return data
}

func (lb GlobalPriorityRequestLoadBalancer) HandleHttpResponseBody(ctx wrapper.HttpContext, body []byte) types.Action {
	return types.ActionContinue
}

func (lb GlobalPriorityRequestLoadBalancer) HandleHttpStreamDone(ctx wrapper.HttpContext) {
	isErr, _ := ctx.GetContext("error").(bool)
	if !isErr {
		routeName, _ := ctx.GetContext("routeName").(string)
		clusterName, _ := ctx.GetContext("clusterName").(string)
		hostSelected, _ := ctx.GetContext("host_selected").(string)
		if hostSelected != "" {
			if err := lb.redisClient.HIncrBy(fmt.Sprintf(RedisKeyFormat, routeName, clusterName), hostSelected, -1, nil); err != nil {
				log.Errorf("failed to decrement host count: %v", err)
			}
		}
		log.Errorf("priority load balancer done: %s", hostSelected)
	}
	log.Debugf("priority load balancer done")
}
