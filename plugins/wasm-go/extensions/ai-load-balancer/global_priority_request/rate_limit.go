package global_priority_request

import (
	"github.com/higress-group/wasm-go/pkg/wrapper"
)

func (lb GlobalPriorityRequestLoadBalancer) checkRateLimit(hostSelected string, currentCount int64, ctx wrapper.HttpContext, routeName string, clusterName string) bool {
	
	return true
}
