/*
Copyright 2024 The HAMi Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package routes

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	"github.com/julienschmidt/httprouter"
	"k8s.io/klog/v2"
	extenderv1 "k8s.io/kube-scheduler/extender/v1"

	"github.com/Project-HAMi/HAMi/pkg/scheduler"
)

const maxRequestSize = 1024 * 1024 // 1MB limit

// checkBody 检查 HTTP 请求体是否存在
// 这是一个辅助函数，用于统一的请求验证
//
// 功能说明：
// - 检查请求体是否为 nil
// - 如果为空，返回 400 Bad Request
//
// 为什么需要这个检查？
// - POST 请求必须有请求体
// - 提前验证避免后续解码错误
// - 提供清晰的错误信息
//
// 参数：
// - w: HTTP 响应写入器
// - r: HTTP 请求对象
func checkBody(w http.ResponseWriter, r *http.Request) {
	if r.Body == nil {
		http.Error(w, "Please send a request body", 400)
		return
	}
}

//+scheduler:2.http
// PredicateRoute 创建 Filter 接口的 HTTP 处理器
// 这是 Kubernetes 调度器调用的过滤接口
//
// 处理流程：
// 1. 检查请求体是否存在
// 2. 限制请求大小（防止 DoS 攻击）
// 3. 解码 ExtenderArgs（包含 Pod 和候选节点）
// 4. 等待缓存同步完成（确保数据完整性）
// 5. 调用 Scheduler.Filter 进行过滤
// 6. 返回 ExtenderFilterResult（选定的节点）
//
// 为什么要限制请求大小？
// - 防止恶意请求消耗大量内存
// - 1MB 足够容纳正常的调度请求
// - 保护调度器的稳定性
//
// 为什么要等待缓存同步？
// - Informer 启动时需要时间同步数据
// - 基于不完整数据调度可能导致错误
// - 确保调度决策的正确性
//
// context 的作用：
// - 传递请求的超时控制
// - 支持优雅取消（客户端断开连接）
// - 避免长时间阻塞
//
// 错误处理：
// - 解码失败：返回 400 Bad Request
// - 缓存未同步：返回 500 Internal Server Error
// - 过滤失败：返回错误信息在 ExtenderFilterResult 中
//
// 参数：
// - s: 调度器实例
//
// 返回值：
// - httprouter.Handle: HTTP 处理函数
func PredicateRoute(s *scheduler.Scheduler) httprouter.Handle {
	klog.Infoln("Initializing Predicate Route")
	return func(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
		klog.Infoln("Entering Predicate Route handler")
		checkBody(w, r)

		var buf bytes.Buffer
		// Limit the body size to prevent deep nesting/resource exhaustion attacks
		limitedReader := io.LimitReader(r.Body, maxRequestSize)
		body := io.TeeReader(limitedReader, &buf)

		var extenderArgs extenderv1.ExtenderArgs
		var extenderFilterResult *extenderv1.ExtenderFilterResult

		if err := json.NewDecoder(body).Decode(&extenderArgs); err != nil {
			klog.ErrorS(err, "Failed to decode extender arguments")
			extenderFilterResult = &extenderv1.ExtenderFilterResult{
				Error: err.Error(),
			}
		} else {
			synced := s.WaitForCacheSync(r.Context())
			if !synced {
				// Poll may return false when context is cancelled
				err := fmt.Errorf("context cancelled")
				klog.ErrorS(err, "Cache not synced, cannot proceed with filtering")
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusInternalServerError)
				w.Write([]byte(err.Error()))
				return
			}
			extenderFilterResult, err = s.Filter(extenderArgs)
			if err != nil {
				klog.ErrorS(err, "Filter error for pod", "pod", extenderArgs.Pod.Name)
				extenderFilterResult = &extenderv1.ExtenderFilterResult{
					Error: err.Error(),
				}
			}
		}

		if resultBody, err := json.Marshal(extenderFilterResult); err != nil {
			klog.ErrorS(err, "Failed to marshal extender filter result", "result", extenderFilterResult)
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte(err.Error()))
		} else {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			w.Write(resultBody)
		}
	}
}

//+scheduler:3.http
// Bind 创建 Bind 接口的 HTTP 处理器
// 这是 Kubernetes 调度器调用的绑定接口
//
// 处理流程：
// 1. 限制请求大小（防止 DoS 攻击）
// 2. 解码 ExtenderBindingArgs（包含 Pod 名称和目标节点）
// 3. 调用 Scheduler.Bind 执行绑定
// 4. 返回 ExtenderBindingResult（绑定结果）
//
// 为什么不需要等待缓存同步？
// - Bind 阶段已经选定了节点
// - 只需要执行绑定操作，不需要查询状态
// - Filter 阶段已经确保了缓存同步
//
// 错误处理：
// - 解码失败：返回错误在 ExtenderBindingResult 中
// - 绑定失败：返回错误在 ExtenderBindingResult 中
// - 序列化失败：返回 500 Internal Server Error
//
// 参数：
// - s: 调度器实例
//
// 返回值：
// - httprouter.Handle: HTTP 处理函数
func Bind(s *scheduler.Scheduler) httprouter.Handle {
	return func(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
		klog.Infoln("Entering Bind handler")
		var buf bytes.Buffer
		// Limit the body size to prevent deep nesting/resource exhaustion attacks
		limitedReader := io.LimitReader(r.Body, maxRequestSize)
		body := io.TeeReader(limitedReader, &buf)
		var extenderBindingArgs extenderv1.ExtenderBindingArgs
		var extenderBindingResult *extenderv1.ExtenderBindingResult

		if err := json.NewDecoder(body).Decode(&extenderBindingArgs); err != nil {
			klog.ErrorS(err, "Failed to decode extender binding arguments")
			extenderBindingResult = &extenderv1.ExtenderBindingResult{
				Error: err.Error(),
			}
		} else {
			extenderBindingResult, err = s.Bind(extenderBindingArgs)
			if err != nil {
				klog.ErrorS(err, "Bind error for pod", "pod", extenderBindingArgs.PodName)
				extenderBindingResult = &extenderv1.ExtenderBindingResult{
					Error: err.Error(),
				}
			}
		}

		if response, err := json.Marshal(extenderBindingResult); err != nil {
			klog.ErrorS(err, "Failed to marshal binding result", "result", extenderBindingResult)
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusInternalServerError)
			errMsg := fmt.Sprintf("{'error':'%s'}", err.Error())
			w.Write([]byte(errMsg))
		} else {
			klog.V(5).InfoS("Returning bind response", "result", extenderBindingResult)
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			w.Write(response)
		}
	}
}

//+scheduler:1.http
// WebHookRoute 创建 Webhook 接口的 HTTP 处理器
// 这是 Kubernetes API Server 调用的准入控制接口
//
// 功能说明：
// - 创建 Webhook 处理器
// - 处理 Pod 的创建和更新请求
// - 返回修改后的 Pod 或拒绝原因
//
// 为什么单独创建 Webhook？
// - Webhook 使用 controller-runtime 的 admission 包
// - 与 Scheduler 的生命周期独立
// - 支持热重载证书
//
// 错误处理：
// - 创建失败时记录错误日志
// - 但不影响路由注册（避免启动失败）
//
// 返回值：
// - httprouter.Handle: HTTP 处理函数
func WebHookRoute() httprouter.Handle {
	h, err := scheduler.NewWebHook()
	if err != nil {
		klog.ErrorS(err, "Failed to create new webhook")
	}
	return func(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
		klog.Infof("Handling webhook request on %s", r.URL.Path)
		h.ServeHTTP(w, r)
	}
}

// HealthzRoute 创建健康检查接口的 HTTP 处理器
// 用于 Kubernetes 的 Liveness 探针
//
// 功能说明：
// - 简单返回 200 OK
// - 表示进程存活
//
// 为什么不检查其他状态？
// - Liveness 只关心进程是否存活
// - 不关心是否准备好处理请求
// - 避免误杀正在启动的实例
//
// 返回值：
// - httprouter.Handle: HTTP 处理函数
func HealthzRoute() httprouter.Handle {
	return func(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
		klog.Infoln("Health check endpoint hit")
		w.WriteHeader(http.StatusOK)
	}
}

// ReadyzRoute 创建就绪检查接口的 HTTP 处理器
// 用于 Kubernetes 的 Readiness 探针
//
// 功能说明：
// - 检查是否是 Leader（如果启用选举）
// - 只有 Leader 才返回 200 OK
// - 非 Leader 返回 503 Service Unavailable
//
// 为什么要检查 Leader？
// - 只有 Leader 处理调度请求
// - 非 Leader 不应该接收流量
// - 支持多副本高可用部署
//
// 与 Healthz 的区别：
// - Healthz: 进程存活检查（不会重启 Pod）
// - Readyz: 服务就绪检查（会从负载均衡中移除）
//
// 参数：
// - s: 调度器实例
//
// 返回值：
// - httprouter.Handle: HTTP 处理函数
func ReadyzRoute(s *scheduler.Scheduler) httprouter.Handle {
	return func(w http.ResponseWriter, r *http.Request, p httprouter.Params) {
		klog.Infoln("Readiness check endpoint hit")

		ok := s.GetLeaderManager().IsLeader()
		if !ok {
			klog.Infoln("Not leader yet")
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}

		klog.Infoln("Scheduler extender is leader")
		w.WriteHeader(http.StatusOK)
	}
}
